use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Range;

use apollo_compiler::collections::IndexMap;
use apollo_compiler::parser::LineColumn;
use itertools::Itertools;

use super::Diagnostic;
use super::Location;
use crate::sources::connect::validation::Severity;

type Locations = Vec<Location>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Shape {
    /// We have no idea what shape this will be, so we can't validate it.
    Any(Locations),
    Bool(Locations),
    Number(Locations),
    String(Locations),
    Array {
        locations: Locations,
        inner: Box<Self>,
    },
    Object {
        locations: Locations,
        attributes: IndexMap<String, Self>,
    },
    /// Either the inner shape or null.
    Nullable(Box<Self>),
    /// One of a number of shapes.
    Union(Vec<Self>),
}

impl Shape {
    pub(super) fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Any(_), typed) | (typed, Self::Any(_)) => typed,
            (Self::Bool(first), Self::Bool(second)) => Self::Bool(combine(first, second)),
            (Self::Number(first), Self::Number(second)) => Self::Number(combine(first, second)),
            (Self::String(first), Self::String(second)) => Self::String(combine(first, second)),
            (
                Self::Array { inner, locations },
                Self::Array {
                    inner: other,
                    locations: other_locations,
                },
            ) => Self::Array {
                inner: Box::new(inner.merge(*other)),
                locations: combine(locations, other_locations),
            },
            (
                Self::Object {
                    attributes,
                    locations,
                },
                Self::Object {
                    attributes: mut other_attributes,
                    locations: other_locations,
                },
            ) => {
                let mut attributes: IndexMap<_, _> = attributes
                    .into_iter()
                    .map(|(key, mut value)| {
                        if let Some(other_inner) = other_attributes.shift_remove(&key) {
                            value = value.merge(other_inner);
                        }
                        (key, value)
                    })
                    .collect();
                attributes.extend(other_attributes);

                Self::Object {
                    attributes,
                    locations: combine(locations, other_locations),
                }
            }
            (Self::Nullable(first), Self::Nullable(second)) => {
                Self::Nullable(Box::from(first.merge(*second)))
            }
            (Self::Nullable(inner), other) | (other, Self::Nullable(inner)) => {
                Self::Nullable(Box::from(inner.merge(other)))
            }
            (Self::Union(inner), Self::Union(other)) => {
                Self::Union(inner.into_iter().chain(other).dedup().collect())
            }
            (Self::Union(inner), other) | (other, Self::Union(inner)) => Self::Union(
                inner
                    .into_iter()
                    .chain(std::iter::once(other))
                    .dedup()
                    .collect(),
            ),
            (first, second) => Self::Union(vec![first, second]),
        }
    }

    pub(super) fn get_key(&self, key: &str) -> Result<Self, Diagnostic> {
        match self {
            Self::Any(_) => Ok(self.clone()),
            Self::Bool(locations) | Self::Number(locations) | Self::String(locations) => {
                Err(Diagnostic::for_locations(
                    locations.iter().cloned(),
                    format!("Can't get key `{key}` from a {self}."),
                    Severity::Error,
                ))
            }
            Self::Array { inner, .. } => inner.get_key(key),
            Self::Object {
                attributes,
                locations,
            } => attributes.get(key).cloned().ok_or_else(|| {
                Diagnostic::for_locations(
                    locations.iter().cloned(),
                    format!("Key `{key}` not found."),
                    Severity::Error,
                )
            }),
            Self::Nullable(inner) => inner.get_key(key),
            Self::Union(inners) => {
                let mut key_union = Vec::with_capacity(inners.len());
                for inner in inners {
                    match inner.get_key(key) {
                        Ok(inner) => key_union.push(inner.clone()),
                        Err(err) => return Err(err),
                    }
                }
                Ok(Shape::Union(key_union))
            }
        }
    }

    pub(super) fn check_compatibility_with(self, other: Self) -> Vec<Diagnostic> {
        use Shape::*;
        match (self, other) {
            (Any(_), _)
            | (_, Any(_))
            | (Bool(_), Bool(_))
            | (Number(_), Number(_))
            | (String(_), String(_)) => vec![],
            (
                Array { inner, .. },
                Array {
                    inner: other_inner, ..
                },
            ) => inner.check_compatibility_with(*other_inner),
            (Union(_), Union(_)) => {
                // TODO: type unions are hard
                vec![]
            }
            (
                Object { attributes, .. },
                Object {
                    attributes: mut other_attributes,
                    ..
                },
            ) => {
                let mut diagnostics = vec![];
                // Check all the attributes we _do_ have
                for (key, value) in attributes {
                    if let Some(other_value) = other_attributes.shift_remove(&key) {
                        diagnostics.extend(
                            value.check_compatibility_with(other_value).into_iter().map(
                                |mut diagnostic| {
                                    diagnostic.message = format!(
                                        "Key `{key}`: {message}",
                                        message = diagnostic.message
                                    );
                                    diagnostic
                                },
                            ),
                        );
                    } else {
                        diagnostics.push(Diagnostic::for_locations(
                            value.locations(),
                            format!("Extra key `{key}` found, but not required."),
                            Severity::Warning,
                        ));
                    }
                }
                for (key, value) in other_attributes {
                    diagnostics.push(Diagnostic::for_locations(
                        value.locations(),
                        format!("Expected key `{key}` not found."),
                        Severity::Error,
                    ));
                }
                diagnostics
            }
            (Nullable(inner), other) | (other, Nullable(inner)) => {
                inner.check_compatibility_with(other)
            }
            (actual, expected) => vec![Diagnostic::for_locations(
                actual.locations().into_iter().chain(expected.locations()),
                format!("Expected {expected}, found {actual}."),
                Severity::Error,
            )],
        }
    }

    pub(crate) fn locations(&self) -> Vec<Location> {
        match self {
            Self::Any(locations) => locations.clone(),
            Self::Bool(locations) => locations.clone(),
            Self::Number(locations) => locations.clone(),
            Self::String(locations) => locations.clone(),
            Self::Array { locations, .. } => locations.clone(),
            Self::Object { locations, .. } => locations.clone(),
            Self::Nullable(inner) => inner.locations(),
            Self::Union(inners) => inners.iter().flat_map(Self::locations).collect(),
        }
    }

    pub(crate) fn add_location(&mut self, location: Location) {
        match self {
            Self::Any(locations)
            | Self::Bool(locations)
            | Self::Number(locations)
            | Self::String(locations)
            | Self::Array { locations, .. }
            | Self::Object { locations, .. } => locations.push(location),
            Self::Nullable(inner) => inner.add_location(location),
            Self::Union(inners) => inners
                .iter_mut()
                .for_each(|inner| inner.add_location(location.clone())),
        }
    }

    pub(super) fn replace_locations(&mut self, locations: Vec<Location>) {
        match self {
            Self::Any(old_locations)
            | Self::Bool(old_locations)
            | Self::Number(old_locations)
            | Self::String(old_locations)
            | Self::Array {
                locations: old_locations,
                ..
            }
            | Self::Object {
                locations: old_locations,
                ..
            } => *old_locations = locations,
            Self::Nullable(inner) => inner.replace_locations(locations),
            Self::Union(inners) => inners
                .iter_mut()
                .for_each(|inner| inner.replace_locations(locations.clone())),
        }
    }

    pub(super) fn is_array(&self) -> bool {
        match self {
            Self::Array { .. } => true,
            Self::Union(inners) => inners.iter().all(Self::is_array),
            _ => false,
        }
    }
}

impl From<&serde_json_bytes::Value> for Shape {
    fn from(value: &serde_json_bytes::Value) -> Self {
        let locations = Vec::new();
        match value {
            // If the value was null, we have no way of knowing what the true shape is.
            serde_json_bytes::Value::Null => Self::Nullable(Box::new(Self::Any(locations))),
            serde_json_bytes::Value::Bool(_) => Self::Bool(locations),
            serde_json_bytes::Value::Number(_) => Self::Number(locations),
            serde_json_bytes::Value::String(_) => Self::String(locations),
            serde_json_bytes::Value::Array(values) => {
                let mut inner = Self::Any(locations.clone());
                for value in values {
                    inner = inner.merge(Shape::from(value));
                }
                Self::Array {
                    inner: Box::new(inner),
                    locations,
                }
            }
            serde_json_bytes::Value::Object(object) => {
                let mut attributes =
                    IndexMap::with_capacity_and_hasher(object.len(), Default::default());
                for (key, value) in object {
                    attributes.insert(key.as_str().to_string(), Shape::from(value));
                }
                Self::Object {
                    attributes,
                    locations,
                }
            }
        }
    }
}

impl Shape {
    pub fn from_json(document_name: String, value: spanned_json_parser::SpannedValue) -> Self {
        use spanned_json_parser::Value::*;
        let location = Location {
            document: document_name.clone(),
            range: Some(Range {
                start: LineColumn {
                    line: value.start.line,
                    column: value.start.col,
                },
                end: LineColumn {
                    line: value.end.line,
                    // Their end col is actually off by one since it's inclusive not exclusive
                    column: value.end.col + 1,
                },
            }),
        };
        let locations = vec![location];
        match value.value {
            Null => Self::Nullable(Box::new(Self::Any(locations))),
            Bool(_) => Self::Bool(locations),
            Number(_) => Self::Number(locations),
            String(_) => Self::String(locations),
            Array(values) => {
                let mut inner = Self::Any(Vec::new());
                for value in values {
                    inner = inner.merge(Self::from_json(document_name.clone(), value));
                }
                Self::Array {
                    inner: Box::new(inner),
                    locations,
                }
            }
            Object(object) => {
                let mut attributes =
                    IndexMap::with_capacity_and_hasher(object.len(), Default::default());
                for (key, value) in object {
                    attributes.insert(key, Shape::from_json(document_name.clone(), value));
                }
                Self::Object {
                    attributes,
                    locations,
                }
            }
        }
    }
}

impl Display for Shape {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Any(_) => write!(f, "Any"),
            Self::Bool(_) => write!(f, "Bool"),
            Self::Number(_) => write!(f, "Number"),
            Self::String(_) => write!(f, "String"),
            Self::Array { inner, .. } => write!(f, "[{}]", inner),
            Self::Object { attributes, .. } => {
                write!(f, "{{")?;
                let mut attributes = attributes.iter();
                if let Some((key, value)) = attributes.next() {
                    write!(f, "{key}: {value}")?;
                }
                for (key, value) in attributes {
                    write!(f, ", {}: {}", key, value)?;
                }
                write!(f, "}}")
            }
            Self::Nullable(inner) => write!(f, "{}?", inner),
            Self::Union(inner) => {
                write!(f, "(")?;
                write!(f, "{}", inner.iter().format(" | "))?;
                write!(f, ")")
            }
        }
    }
}

impl Default for Shape {
    fn default() -> Self {
        Self::Any(Vec::new())
    }
}

fn combine(first: Locations, second: Locations) -> Locations {
    first.into_iter().chain(second).dedup().collect()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use spanned_json_parser::parse;

    use super::*;
    #[test]
    fn spanned_json_locations() {
        let input = r#"{"a": 1, "b": "2"}"#;
        let value = parse(input).unwrap();
        let shape = Shape::from_json("file://test".to_string(), value);
        assert_eq!(
            shape.locations(),
            vec![Location {
                document: "file://test".to_string(),
                range: Some(Range {
                    start: LineColumn { line: 0, column: 0 },
                    end: LineColumn {
                        line: 0,
                        column: 15
                    }
                })
            }]
        );
        let Shape::Object { attributes, .. } = shape else {
            panic!("Expected an object, found {:?}", shape);
        };
        assert_eq!(attributes.len(), 2);
        assert_eq!(
            attributes["a"].locations(),
            vec![Location {
                document: "file://test".to_string(),
                range: Some(Range {
                    start: LineColumn { line: 0, column: 6 },
                    end: LineColumn { line: 0, column: 7 }
                })
            }]
        );
        assert_eq!(
            attributes["b"].locations(),
            vec![Location {
                document: "file://test".to_string(),
                range: Some(Range {
                    start: LineColumn {
                        line: 0,
                        column: 10
                    },
                    end: LineColumn {
                        line: 0,
                        column: 13
                    }
                })
            }]
        );
    }
}