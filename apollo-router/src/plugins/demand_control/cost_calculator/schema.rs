use std::ops::Deref;
use std::sync::Arc;

use ahash::HashMap;
use ahash::HashMapExt;
use apollo_compiler::schema::ExtendedType;
use apollo_compiler::validation::Valid;
use apollo_compiler::Name;
use apollo_compiler::Schema;

use super::directives::get_apollo_directive_names;
use super::directives::CostDirective;
use super::directives::DefinitionListSizeDirective as ListSizeDirective;
use super::directives::RequiresDirective;
use crate::plugins::demand_control::DemandControlError;

pub(crate) struct DemandControlledSchema {
    directive_name_map: HashMap<Name, Name>,
    inner: Arc<Valid<Schema>>,
    type_field_cost_directives: HashMap<Name, HashMap<Name, CostDirective>>,
    type_field_list_size_directives: HashMap<Name, HashMap<Name, ListSizeDirective>>,
    type_field_requires_directives: HashMap<Name, HashMap<Name, RequiresDirective>>,
}

impl DemandControlledSchema {
    pub(crate) fn new(schema: Arc<Valid<Schema>>) -> Result<Self, DemandControlError> {
        let directive_name_map = get_apollo_directive_names(&schema);

        let mut type_field_cost_directives: HashMap<Name, HashMap<Name, CostDirective>> =
            HashMap::new();
        let mut type_field_list_size_directives: HashMap<Name, HashMap<Name, ListSizeDirective>> =
            HashMap::new();
        let mut type_field_requires_directives: HashMap<Name, HashMap<Name, RequiresDirective>> =
            HashMap::new();

        for (type_name, type_) in &schema.types {
            let field_cost_directives = type_field_cost_directives
                .entry(type_name.clone())
                .or_default();
            let field_list_size_directives = type_field_list_size_directives
                .entry(type_name.clone())
                .or_default();
            let field_requires_directives = type_field_requires_directives
                .entry(type_name.clone())
                .or_default();

            match type_ {
                ExtendedType::Interface(ty) => {
                    for field_name in ty.fields.keys() {
                        let field_definition = schema.type_field(type_name, field_name)?;
                        let field_type = schema.types.get(field_definition.ty.inner_named_type()).ok_or_else(|| {
                            DemandControlError::QueryParseFailure(format!(
                                "Field {} was found in query, but its type is missing from the schema.",
                                field_name
                            ))
                        })?;

                        if let Some(cost_directive) =
                            CostDirective::from_field(&directive_name_map, field_definition)
                                .or(CostDirective::from_type(&directive_name_map, field_type))
                        {
                            field_cost_directives.insert(field_name.clone(), cost_directive);
                        }

                        if let Some(list_size_directive) = ListSizeDirective::from_field_definition(
                            &directive_name_map,
                            field_definition,
                            &schema,
                        )? {
                            field_list_size_directives
                                .insert(field_name.clone(), list_size_directive);
                        }

                        if let Some(requires_directive) = RequiresDirective::from_field_definition(
                            field_definition,
                            type_name,
                            &schema,
                        )? {
                            field_requires_directives
                                .insert(field_name.clone(), requires_directive);
                        }
                    }
                }
                ExtendedType::Object(ty) => {
                    for field_name in ty.fields.keys() {
                        let field_definition = schema.type_field(type_name, field_name)?;
                        let field_type = schema.types.get(field_definition.ty.inner_named_type()).ok_or_else(|| {
                            DemandControlError::QueryParseFailure(format!(
                                "Field {} was found in query, but its type is missing from the schema.",
                                field_name
                            ))
                        })?;

                        if let Some(cost_directive) =
                            CostDirective::from_field(&directive_name_map, field_definition)
                                .or(CostDirective::from_type(&directive_name_map, field_type))
                        {
                            field_cost_directives.insert(field_name.clone(), cost_directive);
                        }

                        if let Some(list_size_directive) = ListSizeDirective::from_field_definition(
                            &directive_name_map,
                            field_definition,
                            &schema,
                        )? {
                            field_list_size_directives
                                .insert(field_name.clone(), list_size_directive);
                        }

                        if let Some(requires_directive) = RequiresDirective::from_field_definition(
                            field_definition,
                            type_name,
                            &schema,
                        )? {
                            field_requires_directives
                                .insert(field_name.clone(), requires_directive);
                        }
                    }
                }
                _ => {
                    // Other types don't have fields
                }
            }
        }

        Ok(Self {
            directive_name_map,
            inner: schema,
            type_field_cost_directives,
            type_field_list_size_directives,
            type_field_requires_directives,
        })
    }

    pub(in crate::plugins::demand_control) fn directive_name_map(&self) -> &HashMap<Name, Name> {
        &self.directive_name_map
    }

    pub(in crate::plugins::demand_control) fn type_field_cost_directive(
        &self,
        type_name: &str,
        field_name: &str,
    ) -> Option<&CostDirective> {
        self.type_field_cost_directives
            .get(type_name)?
            .get(field_name)
    }

    pub(in crate::plugins::demand_control) fn type_field_list_size_directive(
        &self,
        type_name: &str,
        field_name: &str,
    ) -> Option<&ListSizeDirective> {
        self.type_field_list_size_directives
            .get(type_name)?
            .get(field_name)
    }

    pub(in crate::plugins::demand_control) fn type_field_requires_directive(
        &self,
        type_name: &str,
        field_name: &str,
    ) -> Option<&RequiresDirective> {
        self.type_field_requires_directives
            .get(type_name)?
            .get(field_name)
    }
}

impl AsRef<Valid<Schema>> for DemandControlledSchema {
    fn as_ref(&self) -> &Valid<Schema> {
        &self.inner
    }
}

impl Deref for DemandControlledSchema {
    type Target = Schema;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::spec::Schema;

    #[test]
    fn list_size_valid_subselection() {
        let schema_str = include_str!("fixtures/list_size_selector_schema.graphql");
        let schema = Schema::parse(&schema_str, &Default::default()).expect("schema parses");
        let arced_schema = Arc::new(schema.supergraph_schema().clone());
        let processed = DemandControlledSchema::new(arced_schema);

        assert!(processed.is_ok())
    }

    #[test]
    fn list_size_selection_path_validation() {
        let schema_str = include_str!("fixtures/invalid_list_size_selector_path.graphql");
        let schema = Schema::parse(&schema_str, &Default::default()).expect("schema parses");
        let arced_schema = Arc::new(schema.supergraph_schema().clone());
        let processed = DemandControlledSchema::new(arced_schema);

        assert_eq!(
            processed.err(),
            Some(DemandControlError::InvalidListSizeApplication(
                "Property .doesntExist not found in object".to_string(),
            ))
        )
    }

    #[test]
    fn list_size_selection_subselection_validation() {
        let schema_str = include_str!("fixtures/invalid_list_size_selector_subselection.graphql");
        let schema = Schema::parse(&schema_str, &Default::default()).expect("schema parses");
        let arced_schema = Arc::new(schema.supergraph_schema().clone());
        let processed = DemandControlledSchema::new(arced_schema);

        assert_eq!(
            processed.err(),
            Some(DemandControlError::InvalidListSizeApplication(
                "Property .nonexistent not found in object".to_string(),
            ))
        )
    }

    #[test]
    fn list_size_selection_non_integer_validation() {
        let schema_str = include_str!("fixtures/invalid_list_size_selects_non_integer.graphql");
        let schema = Schema::parse(&schema_str, &Default::default()).expect("schema parses");
        let arced_schema = Arc::new(schema.supergraph_schema().clone());
        let processed = DemandControlledSchema::new(arced_schema);

        assert_eq!(
            processed.err(),
            Some(DemandControlError::InvalidListSizeApplication(
                "Selected non-Int".to_string(),
            ))
        )
    }
}
