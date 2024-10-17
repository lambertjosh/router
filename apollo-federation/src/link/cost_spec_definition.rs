use apollo_compiler::ast::Argument;
use apollo_compiler::ast::Directive;
use apollo_compiler::ast::FieldDefinition;
use apollo_compiler::collections::IndexMap;
use apollo_compiler::name;
use apollo_compiler::schema::Component;
use apollo_compiler::schema::EnumType;
use apollo_compiler::schema::ExtendedType;
use apollo_compiler::schema::InputObjectType;
use apollo_compiler::schema::ObjectType;
use apollo_compiler::schema::ScalarType;
use apollo_compiler::Name;
use apollo_compiler::Node;
use lazy_static::lazy_static;

use crate::error::FederationError;
use crate::link::spec::Identity;
use crate::link::spec::Url;
use crate::link::spec::Version;
use crate::link::spec_definition::SpecDefinition;
use crate::link::spec_definition::SpecDefinitions;
use crate::schema::position::EnumTypeDefinitionPosition;
use crate::schema::position::ObjectTypeDefinitionPosition;
use crate::schema::position::ScalarTypeDefinitionPosition;
use crate::schema::FederationSchema;
use crate::sources::connect::validation::Code;
use crate::sources::connect::validation::Message;
use crate::sources::connect::ApplyToError;
use crate::sources::connect::JSONSelection;

pub(crate) const COST_DIRECTIVE_NAME_IN_SPEC: Name = name!("cost");
pub(crate) const COST_DIRECTIVE_NAME_DEFAULT: Name = name!("federation__cost");

pub(crate) const LIST_SIZE_DIRECTIVE_NAME_IN_SPEC: Name = name!("listSize");
pub(crate) const LIST_SIZE_DIRECTIVE_NAME_DEFAULT: Name = name!("federation__listSize");
pub(crate) const LIST_SIZE_SLICING_ARGUMENTS_NAME: Name = name!("slicingArguments");

#[derive(Clone)]
pub(crate) struct CostSpecDefinition {
    url: Url,
    minimum_federation_version: Option<Version>,
}

macro_rules! propagate_demand_control_directives {
    ($func_name:ident, $directives_ty:ty, $wrap_ty:expr) => {
        pub(crate) fn $func_name(
            &self,
            subgraph_schema: &FederationSchema,
            source: &$directives_ty,
            dest: &mut $directives_ty,
            original_directive_names: &IndexMap<Name, Name>,
        ) -> Result<(), FederationError> {
            let cost_directive_name = original_directive_names.get(&COST_DIRECTIVE_NAME_IN_SPEC);
            let cost_directive = cost_directive_name.and_then(|name| source.get(name.as_str()));
            if let Some(cost_directive) = cost_directive {
                dest.push($wrap_ty(self.cost_directive(
                    subgraph_schema,
                    cost_directive.arguments.clone(),
                )?));
            }

            let list_size_directive_name =
                original_directive_names.get(&LIST_SIZE_DIRECTIVE_NAME_IN_SPEC);
            let list_size_directive =
                list_size_directive_name.and_then(|name| source.get(name.as_str()));
            if let Some(list_size_directive) = list_size_directive {
                dest.push($wrap_ty(self.list_size_directive(
                    subgraph_schema,
                    list_size_directive.arguments.clone(),
                )?));
            }

            Ok(())
        }
    };
}

macro_rules! propagate_demand_control_directives_to_position {
    ($func_name:ident, $source_ty:ty, $dest_ty:ty) => {
        pub(crate) fn $func_name(
            &self,
            subgraph_schema: &mut FederationSchema,
            source: &Node<$source_ty>,
            dest: &$dest_ty,
            original_directive_names: &IndexMap<Name, Name>,
        ) -> Result<(), FederationError> {
            let cost_directive_name = original_directive_names.get(&COST_DIRECTIVE_NAME_IN_SPEC);
            let cost_directive =
                cost_directive_name.and_then(|name| source.directives.get(name.as_str()));
            if let Some(cost_directive) = cost_directive {
                dest.insert_directive(
                    subgraph_schema,
                    Component::from(
                        self.cost_directive(subgraph_schema, cost_directive.arguments.clone())?,
                    ),
                )?;
            }

            let list_size_directive_name =
                original_directive_names.get(&LIST_SIZE_DIRECTIVE_NAME_IN_SPEC);
            let list_size_directive =
                list_size_directive_name.and_then(|name| source.directives.get(name.as_str()));
            if let Some(list_size_directive) = list_size_directive {
                dest.insert_directive(
                    subgraph_schema,
                    Component::from(self.list_size_directive(
                        subgraph_schema,
                        list_size_directive.arguments.clone(),
                    )?),
                )?;
            }

            Ok(())
        }
    };
}

impl CostSpecDefinition {
    pub(crate) fn new(version: Version, minimum_federation_version: Option<Version>) -> Self {
        Self {
            url: Url {
                identity: Identity::cost_identity(),
                version,
            },
            minimum_federation_version,
        }
    }

    pub(crate) fn cost_directive(
        &self,
        schema: &FederationSchema,
        arguments: Vec<Node<Argument>>,
    ) -> Result<Directive, FederationError> {
        let name = self
            .directive_name_in_schema(schema, &COST_DIRECTIVE_NAME_IN_SPEC)?
            .unwrap_or(COST_DIRECTIVE_NAME_DEFAULT);

        Ok(Directive { name, arguments })
    }

    pub(crate) fn list_size_directive(
        &self,
        schema: &FederationSchema,
        arguments: Vec<Node<Argument>>,
    ) -> Result<Directive, FederationError> {
        let name = self
            .directive_name_in_schema(schema, &LIST_SIZE_DIRECTIVE_NAME_IN_SPEC)?
            .unwrap_or(LIST_SIZE_DIRECTIVE_NAME_DEFAULT);

        Ok(Directive { name, arguments })
    }

    propagate_demand_control_directives!(
        propagate_demand_control_directives,
        apollo_compiler::ast::DirectiveList,
        Node::new
    );
    propagate_demand_control_directives!(
        propagate_demand_control_schema_directives,
        apollo_compiler::schema::DirectiveList,
        Component::from
    );

    propagate_demand_control_directives_to_position!(
        propagate_demand_control_directives_for_enum,
        EnumType,
        EnumTypeDefinitionPosition
    );
    propagate_demand_control_directives_to_position!(
        propagate_demand_control_directives_for_object,
        ObjectType,
        ObjectTypeDefinitionPosition
    );
    propagate_demand_control_directives_to_position!(
        propagate_demand_control_directives_for_scalar,
        ScalarType,
        ScalarTypeDefinitionPosition
    );

    pub fn validate_schema(schema: &apollo_compiler::Schema) -> Vec<Message> {
        let mut validation_errors = Vec::new();
        for (type_name, type_) in &schema.types {
            match type_ {
                ExtendedType::Interface(ty) => {
                    for field_name in ty.fields.keys() {
                        if let Ok(definition) = schema.type_field(type_name.as_str(), field_name) {
                            if let Some(directive) = definition.directives.get("listSize") {
                                let coordinate = &format!("{}.{}", ty.name, definition.name);
                                validation_errors.extend(Self::validate_list_size_directive(
                                    coordinate, definition, directive, schema,
                                ));
                            }
                        }
                    }
                }
                ExtendedType::Object(ty) => {
                    for field_name in ty.fields.keys() {
                        if let Ok(definition) = schema.type_field(type_name.as_str(), field_name) {
                            if let Some(directive) = definition.directives.get("listSize") {
                                let coordinate = &format!("{}.{}", ty.name, definition.name);
                                validation_errors.extend(Self::validate_list_size_directive(
                                    coordinate, definition, directive, schema,
                                ));
                            }
                        }
                    }
                }
                _ => {
                    // Other types don't have fields
                }
            }
        }
        validation_errors
    }

    fn validate_list_size_directive(
        coordinate: &String,
        definition: &FieldDefinition,
        directive: &Node<Directive>,
        schema: &apollo_compiler::Schema,
    ) -> Vec<Message> {
        let mut validation_failures = Vec::new();
        let error_location = ValidationErrorLocation::new(coordinate, directive, schema);

        if let Ok(Some(slicing_arguments)) = directive
            .argument_by_name(&LIST_SIZE_SLICING_ARGUMENTS_NAME, schema)
            .map(|arg| arg.as_list())
        {
            let as_json = Self::json_representation_of_arguments(definition, schema);
            for slicing_argument in slicing_arguments.iter().filter_map(|arg| arg.as_str()) {
                match JSONSelection::parse(slicing_argument) {
                    Ok((_, selection)) => {
                        let (selected, apply_errors) = selection.apply_to(&as_json);
                        validation_failures.extend(
                            apply_errors
                                .iter()
                                .map(|e| error_location.json_selection_application_error(e)),
                        );

                        if let Some(s) = selected.as_ref() {
                            for lt in Self::leaves(s) {
                                if lt.as_str() != Some("Int") {
                                    validation_failures.push(
                                        error_location.expected_integer_selection_error(
                                            slicing_argument,
                                            lt.as_str().unwrap_or_default(),
                                        ),
                                    );
                                }
                            }
                        }
                    }
                    Err(err) => {
                        validation_failures.push(error_location.json_selection_parse_error(err))
                    }
                }
            }
        }

        validation_failures
    }

    /// Combines the arguments of `definition` into a single JSON object with each argument as an entry.
    fn json_representation_of_arguments(
        definition: &FieldDefinition,
        schema: &apollo_compiler::Schema,
    ) -> serde_json_bytes::Value {
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
        Self::to_json_schema_tree(Some(&combined), schema)
    }

    /// Converts a schema type to a JSON object with leaves that are the names of the type of each terminal scalar.
    ///
    /// **Disclaimer:** This is only implemented for InputObject because we're evaluating selections on inputs. Do not
    /// this as-is for output types, it will result in unexpected null values.
    fn to_json_schema_tree(
        ty: Option<&ExtendedType>,
        schema: &apollo_compiler::Schema,
    ) -> serde_json_bytes::Value {
        use serde_json_bytes::Value as JsonValue;

        match ty {
            Some(ExtendedType::Scalar(node)) => JsonValue::String(node.name.as_str().into()),
            Some(ExtendedType::Enum(node)) => JsonValue::String(node.name.as_str().into()),
            Some(ExtendedType::InputObject(node)) => {
                let mut m: Vec<(serde_json_bytes::ByteString, JsonValue)> = Vec::new();
                for (n, v) in node.fields.iter() {
                    let next_ty = schema.types.get(v.ty.inner_named_type());
                    let converted = Self::to_json_schema_tree(next_ty, schema);
                    m.push((n.as_str().into(), converted))
                }
                JsonValue::Object(m.iter().cloned().collect())
            }
            None
            | Some(ExtendedType::Object(_))
            | Some(ExtendedType::Interface(_))
            | Some(ExtendedType::Union(_)) => {
                // We currently only apply this to input objects, so these three types aren't legal
                JsonValue::Null
            }
        }
    }

    fn leaves(json: &serde_json_bytes::Value) -> Vec<serde_json_bytes::Value> {
        match json {
            serde_json_bytes::Value::Null => vec![json.clone()],
            serde_json_bytes::Value::Bool(_) => vec![json.clone()],
            serde_json_bytes::Value::Number(_) => vec![json.clone()],
            serde_json_bytes::Value::String(_) => vec![json.clone()],
            serde_json_bytes::Value::Array(vec) => vec.iter().flat_map(Self::leaves).collect(),
            serde_json_bytes::Value::Object(map) => map.values().flat_map(Self::leaves).collect(),
        }
    }
}

impl SpecDefinition for CostSpecDefinition {
    fn url(&self) -> &Url {
        &self.url
    }

    fn minimum_federation_version(&self) -> Option<&Version> {
        self.minimum_federation_version.as_ref()
    }
}

lazy_static! {
    pub(crate) static ref COST_VERSIONS: SpecDefinitions<CostSpecDefinition> = {
        let mut definitions = SpecDefinitions::new(Identity::cost_identity());
        definitions.add(CostSpecDefinition::new(
            Version { major: 0, minor: 1 },
            Some(Version { major: 2, minor: 9 }),
        ));
        definitions
    };
}

struct ValidationErrorLocation<'a> {
    coordinate: &'a String,
    directive: &'a Node<Directive>,
    schema: &'a apollo_compiler::Schema,
}

impl<'a> ValidationErrorLocation<'a> {
    fn new(
        coordinate: &'a String,
        directive: &'a Node<Directive>,
        schema: &'a apollo_compiler::Schema,
    ) -> Self {
        Self {
            coordinate,
            directive,
            schema,
        }
    }

    fn json_selection_application_error(&self, error: &ApplyToError) -> Message {
        Message {
            code: Code::SelectedFieldNotFound,
            message: format!(
                "@{}({}:) on {} selects invalid slicing arguments. {}",
                self.directive.name,
                LIST_SIZE_SLICING_ARGUMENTS_NAME,
                self.coordinate,
                error.message()
            ),
            locations: self
                .directive
                .line_column_range(&self.schema.sources)
                .into_iter()
                .collect(),
        }
    }

    fn json_selection_parse_error(&self, error: nom::Err<nom::error::Error<&str>>) -> Message {
        Message {
            code: Code::InvalidJsonSelection,
            message: format!(
                "@{}(slicingArguments:) on {} is not a valid JSONSelection. {}",
                self.directive.name,
                self.coordinate,
                error.to_string(),
            ),
            locations: self
                .directive
                .line_column_range(&self.schema.sources)
                .into_iter()
                .collect(),
        }
    }

    fn expected_integer_selection_error(&self, slicing_argument: &str, value: &str) -> Message {
        // TODO: Different code?
        Message {
            code: Code::InvalidJsonSelection,
            message: format!(
                "@{}({}:) on {} selects invalid slicing arguments. Selection {} selects {}, but Int is required",
                self.directive.name, LIST_SIZE_SLICING_ARGUMENTS_NAME, self.coordinate, slicing_argument, value
            ),
            locations: self.directive
                .line_column_range(&self.schema.sources)
                .into_iter()
                .collect(),
        }
    }
}

#[cfg(test)]
mod test {
    use apollo_compiler::Schema;

    use super::CostSpecDefinition;

    #[test]
    fn valid_selections() {
        let schema_str = include_str!("fixtures/cost_spec_valid_selections.graphqls");
        let schema = Schema::parse(schema_str, "cost_spec_valid_selections.graphqls")
            .expect("schema parses");
        let validation_errors = CostSpecDefinition::validate_schema(&schema);

        assert!(validation_errors.is_empty())
    }

    #[test]
    fn rejects_non_integer_leaves() {
        let schema_str = include_str!("fixtures/cost_spec_invalid_selections.graphqls");
        let schema = Schema::parse(schema_str, "cost_spec_invalid_selections.graphqls")
            .expect("schema parses");
        let validation_errors: Vec<String> = CostSpecDefinition::validate_schema(&schema)
            .iter()
            .map(|m| m.message.clone())
            .collect();

        assert_eq!(
            validation_errors,
            vec![
                "@listSize(slicingArguments:) on Query.getStrings selects invalid slicing arguments. Property .doesntExist not found in object",
                "@listSize(slicingArguments:) on Query.getStrings selects invalid slicing arguments. Selection paging.opaqueCursor selects String, but Int is required",
                "@listSize(slicingArguments:) on Query.getStrings is not a valid JSONSelection. Parsing Error: Error { input: \"}bad selection\", code: IsNot }"
            ]
        )
    }
}
