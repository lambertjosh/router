use std::borrow::Cow;

use futures::future::BoxFuture;
use itertools::Itertools;
use sys_info::hostname;
use tonic::metadata::MetadataMap;
use tower::BoxError;
use opentelemetry::{sdk::{export::trace::{ExportResult, SpanData, SpanExporter}, trace::EvictedQueue, Resource}, trace::{SpanContext, Status, TraceFlags, TraceState}, InstrumentationLibrary, KeyValue};
use url::Url;
use uuid::Uuid;
use crate::plugins::telemetry::{apollo_exporter::get_uname, metrics::apollo::ROUTER_ID, tracing::BatchProcessorConfig, GLOBAL_TRACER_NAME};

use super::tracing::apollo_telemetry::LightSpanData;
    
/// The Apollo Otlp exporter is a thin wrapper around the OTLP SpanExporter.
pub(crate) struct ApolloOtlpExporter {
  batch_config: BatchProcessorConfig,
  endpoint: Url,
  apollo_key: String,
  resource_template: Resource,
  intrumentation_library: InstrumentationLibrary,
  otlp_exporter: Box<dyn SpanExporter>,
}

impl ApolloOtlpExporter {
  pub(crate) fn new(
      endpoint: &Url,
      batch_config: &BatchProcessorConfig,
      apollo_key: &str,
      apollo_graph_ref: &str,
      schema_id: &str,
  ) -> Result<ApolloOtlpExporter, BoxError> {
      tracing::debug!(endpoint = %endpoint, "creating Apollo OTLP traces exporter");
      
      let mut metadata = MetadataMap::new();
      metadata.insert("apollo.api.key", apollo_key.to_string());
      
      return Ok(Self {
        endpoint: endpoint.clone(),
        batch_config: batch_config.clone(),
        apollo_key: apollo_key.to_string(),
        resource_template: Resource::new([
          KeyValue::new("apollo.router.id", ROUTER_ID.get_or_init(Uuid::new_v4).to_string()),
          KeyValue::new("apollo.graph.ref", apollo_graph_ref.to_string()),
          KeyValue::new("apollo.schema.id", schema_id.to_string()),
          KeyValue::new("apollo.user.agent", format!("{}@{}", std::env!("CARGO_PKG_NAME"), std::env!("CARGO_PKG_VERSION"))),
          KeyValue::new("apollo.client.host", hostname()?),
          KeyValue::new("apollo.client.uname", get_uname()?),
        ]),
        intrumentation_library: InstrumentationLibrary::new(
            GLOBAL_TRACER_NAME.clone(), // can this be .to_string()?  what's the difference?
            Some(format!("{}@{}", std::env!("CARGO_PKG_NAME"), std::env!("CARGO_PKG_VERSION"))),
            None,
            None,
        ),
        otlp_exporter: Box::new(
          opentelemetry_otlp::new_exporter()
            .tonic()
            .with_timeout(batch_config.max_export_timeout)
            .with_endpoint(endpoint.to_string())
            .with_metadata(metadata)
            .with_compression(opentelemetry_otlp::Compression::Gzip)
            .build_span_exporter()?
        ),
        // TBD(tim): do we need another batch processor for this?
        // Seems like we've already set up a batcher earlier in the pipe but not quite sure.
      });
  }

  pub(crate) async fn submit_trace_batch(&mut self, traces: Vec<Vec<LightSpanData>>) -> BoxFuture<'static, ExportResult> {
    let spans = traces.into_iter().flat_map(|t| {
      t.into_iter().map(|s| {
        SpanData {
          span_context: SpanContext::new(
            s.trace_id,
            s.span_id,
            TraceFlags::default().with_sampled(true),
            true,
            TraceState::default(),
          ),
          parent_span_id: s.parent_span_id,
          span_kind: s.span_kind,
          name: s.name,
          start_time: s.start_time,
          end_time: s.end_time,
          attributes: s.attributes,
          events: EvictedQueue::new(0),
          links: EvictedQueue::new(0),
          status: Status::Unset,
          resource: Cow::Owned(self.resource_template.to_owned()), // Hooray, Cows!  This might need a look.
          instrumentation_lib: self.intrumentation_library.clone(),
        }
      })
    }).collect_vec();
    self.otlp_exporter.export(spans)
  }
}