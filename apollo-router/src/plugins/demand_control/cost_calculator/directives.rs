use ahash::HashMap;
use ahash::HashMapExt;
use ahash::HashSet;
use apollo_compiler::ast::DirectiveList;
use apollo_compiler::ast::FieldDefinition;
use apollo_compiler::ast::InputValueDefinition;
use apollo_compiler::ast::NamedType;
use apollo_compiler::executable::Field;
use apollo_compiler::executable::FieldSet;
use apollo_compiler::executable::SelectionSet;
use apollo_compiler::name;
use apollo_compiler::parser::Parser;
use apollo_compiler::schema::Component;
use apollo_compiler::schema::ExtendedType;
use apollo_compiler::schema::InputObjectType;
use apollo_compiler::validation::Valid;
use apollo_compiler::Name;
use apollo_compiler::Node;
use apollo_compiler::Schema;
use apollo_federation::link::spec::APOLLO_SPEC_DOMAIN;
use apollo_federation::link::Link;
use apollo_federation::sources::connect::ApplyTo;
use apollo_federation::sources::connect::JSONSelection;
use itertools::Itertools;
use tower::BoxError;

use crate::json_ext::Object;
use crate::json_ext::ValueExt;
use crate::plugins::demand_control::DemandControlError;

const COST_DIRECTIVE_NAME: Name = name!("cost");
const COST_DIRECTIVE_DEFAULT_NAME: Name = name!("federation__cost");
const COST_DIRECTIVE_WEIGHT_ARGUMENT_NAME: Name = name!("weight");

const LIST_SIZE_DIRECTIVE_NAME: Name = name!("listSize");
const LIST_SIZE_DIRECTIVE_DEFAULT_NAME: Name = name!("federation__listSize");
const LIST_SIZE_DIRECTIVE_ASSUMED_SIZE_ARGUMENT_NAME: Name = name!("assumedSize");
const LIST_SIZE_DIRECTIVE_SLICING_ARGUMENTS_ARGUMENT_NAME: Name = name!("slicingArguments");
const LIST_SIZE_DIRECTIVE_SIZED_FIELDS_ARGUMENT_NAME: Name = name!("sizedFields");
const LIST_SIZE_DIRECTIVE_REQUIRE_ONE_SLICING_ARGUMENT_ARGUMENT_NAME: Name =
    name!("requireOneSlicingArgument");

pub(in crate::plugins::demand_control) fn get_apollo_directive_names(
    schema: &Schema,
) -> HashMap<Name, Name> {
    let mut hm: HashMap<Name, Name> = HashMap::new();
    for directive in &schema.schema_definition.directives {
        if directive.name.as_str() == "link" {
            if let Ok(link) = Link::from_directive_application(directive) {
                if link.url.identity.domain != APOLLO_SPEC_DOMAIN {
                    continue;
                }
                for import in link.imports {
                    hm.insert(import.element.clone(), import.imported_name().clone());
                }
            }
        }
    }
    hm
}

pub(in crate::plugins::demand_control) struct CostDirective {
    weight: i32,
}

impl CostDirective {
    pub(in crate::plugins::demand_control) fn weight(&self) -> f64 {
        self.weight as f64
    }

    pub(in crate::plugins::demand_control) fn from_argument(
        directive_name_map: &HashMap<Name, Name>,
        argument: &InputValueDefinition,
    ) -> Option<Self> {
        Self::from_directives(directive_name_map, &argument.directives)
    }

    pub(in crate::plugins::demand_control) fn from_field(
        directive_name_map: &HashMap<Name, Name>,
        field: &FieldDefinition,
    ) -> Option<Self> {
        Self::from_directives(directive_name_map, &field.directives)
    }

    pub(in crate::plugins::demand_control) fn from_type(
        directive_name_map: &HashMap<Name, Name>,
        ty: &ExtendedType,
    ) -> Option<Self> {
        Self::from_schema_directives(directive_name_map, ty.directives())
    }

    fn from_directives(
        directive_name_map: &HashMap<Name, Name>,
        directives: &DirectiveList,
    ) -> Option<Self> {
        directive_name_map
            .get(&COST_DIRECTIVE_NAME)
            .and_then(|name| directives.get(name))
            .or(directives.get(&COST_DIRECTIVE_DEFAULT_NAME))
            .and_then(|cost| cost.specified_argument_by_name(&COST_DIRECTIVE_WEIGHT_ARGUMENT_NAME))
            .and_then(|weight| weight.to_i32())
            .map(|weight| Self { weight })
    }

    pub(in crate::plugins::demand_control) fn from_schema_directives(
        directive_name_map: &HashMap<Name, Name>,
        directives: &apollo_compiler::schema::DirectiveList,
    ) -> Option<Self> {
        directive_name_map
            .get(&COST_DIRECTIVE_NAME)
            .and_then(|name| directives.get(name))
            .or(directives.get(&COST_DIRECTIVE_DEFAULT_NAME))
            .and_then(|cost| cost.specified_argument_by_name(&COST_DIRECTIVE_WEIGHT_ARGUMENT_NAME))
            .and_then(|weight| weight.to_i32())
            .map(|weight| Self { weight })
    }
}

pub(in crate::plugins::demand_control) struct IncludeDirective {
    pub(in crate::plugins::demand_control) is_included: bool,
}

impl IncludeDirective {
    pub(in crate::plugins::demand_control) fn from_field(
        field: &Field,
    ) -> Result<Option<Self>, BoxError> {
        let directive = field
            .directives
            .get("include")
            .and_then(|skip| skip.specified_argument_by_name("if"))
            .and_then(|arg| arg.to_bool())
            .map(|cond| Self { is_included: cond });

        Ok(directive)
    }
}

pub(in crate::plugins::demand_control) struct ListSizeDirective<'schema> {
    pub(in crate::plugins::demand_control) expected_size: Option<i32>,
    pub(in crate::plugins::demand_control) sized_fields: Option<HashSet<&'schema str>>,
}

impl<'schema> ListSizeDirective<'schema> {
    pub(in crate::plugins::demand_control) fn size_of(&self, field: &Field) -> Option<i32> {
        if self
            .sized_fields
            .as_ref()
            .is_some_and(|sf| sf.contains(field.name.as_str()))
        {
            self.expected_size
        } else {
            None
        }
    }
}

fn to_json_with_variables(
    val: &apollo_compiler::ast::Value,
    variables: &Object,
) -> serde_json_bytes::Value {
    use apollo_compiler::ast::Value as AstValue;
    use serde_json_bytes::Value as JsonValue;

    match val {
        AstValue::Null => JsonValue::Null,
        AstValue::Enum(_name) => todo!(),
        AstValue::Variable(name) => variables
            .get(name.as_str())
            .cloned()
            .unwrap_or(JsonValue::Null),
        AstValue::String(s) => JsonValue::String(s.clone().into()),
        AstValue::Float(float_value) => {
            if let Ok(_f) = float_value.try_to_f64() {
                todo!() // JsonValue::Number(f.into())
            } else {
                JsonValue::Null
            }
        }
        AstValue::Int(int_value) => {
            if let Ok(i) = int_value.try_to_i32() {
                JsonValue::Number(i.into())
            } else {
                JsonValue::Null
            }
        }
        AstValue::Boolean(b) => JsonValue::Bool(*b),
        AstValue::List(vec) => JsonValue::Array(
            vec.iter()
                .map(|v| to_json_with_variables(v, variables))
                .collect(),
        ),
        AstValue::Object(vec) => JsonValue::Object(
            vec.iter()
                .map(|(n, v)| (n.as_str().into(), to_json_with_variables(v, variables)))
                .collect(),
        ),
    }
}

fn to_json_with_variables_2(ty: Option<&ExtendedType>, schema: &Schema) -> serde_json_bytes::Value {
    use serde_json_bytes::Value as JsonValue;

    if let Some(ty) = ty {
        match ty {
            ExtendedType::Scalar(node) => JsonValue::String(node.name.as_str().into()),
            ExtendedType::Object(node) => todo!(),
            ExtendedType::Interface(node) => todo!(),
            ExtendedType::Union(node) => todo!(),
            ExtendedType::Enum(node) => todo!(),
            ExtendedType::InputObject(node) => {
                let mut m: Vec<(serde_json_bytes::ByteString, JsonValue)> = Vec::new();
                for (n, v) in node.fields.iter() {
                    let next_ty = schema.types.get(v.ty.inner_named_type());
                    let converted = to_json_with_variables_2(next_ty, schema);
                    m.push((n.as_str().into(), converted))
                }
                JsonValue::Object(m.iter().cloned().collect())
            }
        }
    } else {
        JsonValue::Null
    }
}

fn leaves(json: &serde_json_bytes::Value) -> Vec<serde_json_bytes::Value> {
    match json {
        serde_json_bytes::Value::Null => vec![json.clone()],
        serde_json_bytes::Value::Bool(_) => vec![json.clone()],
        serde_json_bytes::Value::Number(_) => vec![json.clone()],
        serde_json_bytes::Value::String(_) => vec![json.clone()],
        serde_json_bytes::Value::Array(vec) => vec.iter().flat_map(leaves).collect(),
        serde_json_bytes::Value::Object(map) => map.values().flat_map(leaves).collect(),
    }
}

/// The `@listSize` directive from a field definition, which can be converted to
/// `ListSizeDirective` with a concrete field from a request.
pub(in crate::plugins::demand_control) struct DefinitionListSizeDirective {
    assumed_size: Option<i32>,
    slicing_argument_fieldsets: Option<Vec<FieldSet>>,
    slicing_argument_selections: Option<Vec<JSONSelection>>,
    sized_fields: Option<HashSet<String>>,
    require_one_slicing_argument: bool,
}

impl DefinitionListSizeDirective {
    pub(in crate::plugins::demand_control) fn from_field_definition(
        directive_name_map: &HashMap<Name, Name>,
        definition: &FieldDefinition,
        schema: &Valid<Schema>,
    ) -> Result<Option<Self>, DemandControlError> {
        let directive = directive_name_map
            .get(&LIST_SIZE_DIRECTIVE_NAME)
            .and_then(|name| definition.directives.get(name))
            .or(definition.directives.get(&LIST_SIZE_DIRECTIVE_DEFAULT_NAME));
        if let Some(directive) = directive {
            let assumed_size = directive
                .specified_argument_by_name(&LIST_SIZE_DIRECTIVE_ASSUMED_SIZE_ARGUMENT_NAME)
                .and_then(|arg| arg.to_i32());

            /*
            let slicing_argument_fieldsets = directive
                .specified_argument_by_name(&LIST_SIZE_DIRECTIVE_SLICING_ARGUMENTS_ARGUMENT_NAME)
                .and_then(|arg| arg.as_list())
                .map(|arg_list| {
                    arg_list
                        .iter()
                        .flat_map(|arg| arg.as_str())
                        .map(|s| {
                            let (arg_name, field_set) = s.split_once(' ').expect("can split");
                            let arg_type = definition
                                .argument_by_name(arg_name)
                                .expect("has argument")
                                .ty
                                .inner_named_type();
                            FieldSet::parse(schema, arg_type.clone(), field_set, "")
                                .expect("can parse selection")
                        })
                        .collect()
                });
            if let Some(selections) = &slicing_argument_fieldsets {
                let combined = ExtendedType::InputObject(Node::new(InputObjectType {
                    description: None,
                    name: name!("combined"),
                    directives: Default::default(),
                    fields: definition
                        .arguments
                        .iter()
                        .map(|arg| (arg.name.clone(), Component::from((**arg).clone())))
                        .collect(),
                }));
                for selection in selections {
                    Self::validate_fieldset_selects_integer_fields(selection, &combined, schema)?;
                }
            }
            */

            let slicing_argument_selections = directive
                .specified_argument_by_name(&LIST_SIZE_DIRECTIVE_SLICING_ARGUMENTS_ARGUMENT_NAME)
                .and_then(|arg| arg.as_list())
                .map(|arg_list| {
                    arg_list
                        .iter()
                        .flat_map(|arg| arg.as_str())
                        .map(|s| JSONSelection::parse(s).expect("can parse selection").1)
                        .collect()
                });

            if let Some(selections) = &slicing_argument_selections {
                let combined = ExtendedType::InputObject(Node::new(InputObjectType {
                    description: None,
                    name: name!("combined"),
                    directives: Default::default(),
                    fields: definition
                        .arguments
                        .iter()
                        .map(|arg| (arg.name.clone(), Component::from((**arg).clone())))
                        .collect(),
                }));
                for selection in selections {
                    Self::validate_json_selects_integer_fields(selection, &combined, &schema)?;
                }
            }

            let sized_fields = directive
                .specified_argument_by_name(&LIST_SIZE_DIRECTIVE_SIZED_FIELDS_ARGUMENT_NAME)
                .and_then(|arg| arg.as_list())
                .map(|arg_list| {
                    arg_list
                        .iter()
                        .flat_map(|arg| arg.as_str())
                        .map(String::from)
                        .collect()
                });
            let require_one_slicing_argument = directive
                .specified_argument_by_name(
                    &LIST_SIZE_DIRECTIVE_REQUIRE_ONE_SLICING_ARGUMENT_ARGUMENT_NAME,
                )
                .and_then(|arg| arg.to_bool())
                .unwrap_or(true);

            Ok(Some(Self {
                assumed_size,
                slicing_argument_fieldsets: None,
                slicing_argument_selections,
                sized_fields,
                require_one_slicing_argument,
            }))
        } else {
            Ok(None)
        }
    }

    fn validate_json_selects_integer_fields(
        selection: &JSONSelection,
        definition: &ExtendedType,
        schema: &Schema,
    ) -> Result<(), DemandControlError> {
        let json_def = to_json_with_variables_2(Some(definition), schema);
        let (selected, errors) = selection.apply_to(&json_def);
        if errors.len() > 0 {
            return Err(DemandControlError::InvalidListSizeApplication(
                errors.iter().flat_map(|e| e.message()).join(", "),
            ));
        }

        match selected {
            Some(value) => {
                let selected_all_integers = leaves(&value)
                    .iter()
                    .all(|leaf| leaf.as_str() == Some("Int"));
                if selected_all_integers {
                    Ok(())
                } else {
                    Err(DemandControlError::InvalidListSizeApplication(
                        "Selected non-Int".to_string(),
                    ))
                }
            }
            None => Err(DemandControlError::InvalidListSizeApplication(
                "Empty selection".to_string(),
            )),
        }
    }

    fn validate_fieldset_selects_integer_fields(
        selection: &FieldSet,
        definition: &ExtendedType,
        schema: &Schema,
    ) -> Result<(), DemandControlError> {
        todo!()
    }

    pub(in crate::plugins::demand_control) fn with_field_and_variables(
        &self,
        field: &Field,
        variables: &Object,
    ) -> Result<ListSizeDirective, DemandControlError> {
        let mut slicing_arguments: Vec<i32> = Vec::new();

        if let Some(slicing_argument_selections) = self.slicing_argument_selections.as_ref() {
            let combined_arguments = apollo_compiler::ast::Value::Object(
                field
                    .arguments
                    .iter()
                    .map(|arg| (arg.name.clone(), arg.value.clone()))
                    .collect(),
            );
            let combined_arguments = to_json_with_variables(&combined_arguments, variables);

            for selection in slicing_argument_selections {
                let argument_selection = selection.apply_with_vars(
                    &combined_arguments,
                    &variables
                        .iter()
                        .map(|(s, v)| (s.as_str().to_string(), v.clone()))
                        .collect(),
                );
                if let Some(json) = &argument_selection.0 {
                    slicing_arguments.extend(leaves(json).iter().flat_map(|leaf| leaf.as_i32()));
                }
            }

            if self.require_one_slicing_argument && slicing_arguments.len() != 1 {
                return Err(DemandControlError::QueryParseFailure(format!(
                    "Exactly one slicing argument is required, but found {}",
                    slicing_arguments.len()
                )));
            }
        }

        let expected_size = slicing_arguments
            .iter()
            .max()
            .cloned()
            .or(self.assumed_size);

        Ok(ListSizeDirective {
            expected_size,
            sized_fields: self
                .sized_fields
                .as_ref()
                .map(|set| set.iter().map(|s| s.as_str()).collect()),
        })
    }
}

pub(in crate::plugins::demand_control) struct RequiresDirective {
    pub(in crate::plugins::demand_control) fields: SelectionSet,
}

impl RequiresDirective {
    pub(in crate::plugins::demand_control) fn from_field_definition(
        definition: &FieldDefinition,
        parent_type_name: &NamedType,
        schema: &Valid<Schema>,
    ) -> Result<Option<Self>, DemandControlError> {
        let requires_arg = definition
            .directives
            .get("join__field")
            .and_then(|requires| requires.specified_argument_by_name("requires"))
            .and_then(|arg| arg.as_str());

        if let Some(arg) = requires_arg {
            let field_set =
                Parser::new().parse_field_set(schema, parent_type_name.clone(), arg, "")?;

            Ok(Some(RequiresDirective {
                fields: field_set.selection_set.clone(),
            }))
        } else {
            Ok(None)
        }
    }
}

pub(in crate::plugins::demand_control) struct SkipDirective {
    pub(in crate::plugins::demand_control) is_skipped: bool,
}

impl SkipDirective {
    pub(in crate::plugins::demand_control) fn from_field(
        field: &Field,
    ) -> Result<Option<Self>, BoxError> {
        let directive = field
            .directives
            .get("skip")
            .and_then(|skip| skip.specified_argument_by_name("if"))
            .and_then(|arg| arg.to_bool())
            .map(|cond| Self { is_skipped: cond });

        Ok(directive)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use apollo_federation::sources::connect::JSONSelection;
    use test_log::test;

    #[test]
    fn list_size_json_handling() {
        let selector = JSONSelection::parse("first").expect("parses").1;
        let value = serde_json_bytes::json!({
            "first": 5
        });
        let selected = selector.apply_to(&value).0.unwrap_or_default();
        let leaves = leaves(&selected);
        let integer_leaves: Vec<i32> = leaves.iter().flat_map(|l| l.as_i32()).collect();

        assert_eq!(integer_leaves, vec![5]);
    }
}
