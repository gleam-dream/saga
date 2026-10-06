# Actions carry execution correlation and named Duration bounds

<a id="adr-0006"></a>

## Decision

- Every action and event has one Sinal Correlation. Local default is unique(); first durable drive saves explicit correlation or from_key(id), retained across handles.
- EffectKey is opaque with accessors; callback request records remain public and are read by label.
- Public time uses Duration; After(Duration) and explicit Infinity represent optional bounds. Validate complete configuration before execution.

## Rationale and alternatives

- Manually threading correlation caused unattributed application effects. Action context already owns the facts needed to expose it.
- Optional correlation has no useful absence case because both lifetimes have a derivable default. Integer milliseconds lose unit meaning across applications.
- Opaque keys support evolution without positional breaks; public callback records retain simple native field reads. A default run deadline would cut healthy long finite workflows.

## Evidence and history

- Duration: `af644b0eaabb9fef223e35f6378669e7fbc787f5`, 2026-10-03. Step context, opaque/saved keys, mandatory correlation: `499879df5596aeceddaee034fc80dd59482b052b`, `5c0d30b301a3ce672725d32e94a19c593ae35d24`, `9a0b1d807b74159dce55daa9f2a6d7ce25dd9c9b`.
- Format 2 stores correlation; format 1 reads as from_key(id). This evidenced compatibility rule is not a generic migration facility.
- Evidence: Saga/execution/durable/telemetry source and public correlation consumers. Cross-package rationale: Oversight release decisions/defaults and Round 4/5. The unpublished Duration and action-context guides documented these caller migrations.
