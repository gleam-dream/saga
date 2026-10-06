# Owned reporting preserves full typed reports before consumer projection

<a id="adr-0007"></a>

## Decision

- run_owned and its stopped-owner callback retain Result(Outcome(output,error,undo_error), reporting.Error). Reporting owns delivery; application projection uses outcome ports.
- Typed operational causes remain available through accessors. NotStarted requires proven prelaunch rejection; launch/delivery loss stays Unknown.
- One private evidence projection supplies classification and complete safe summaries; Outcome remains the public evidence authority.
- Independent receiver remains until per-invocation owner exit after synchronous return; guarded notification is bounded.

## Rationale and alternatives

- Flat output/diagnostic failures erased native error/undo evidence and confused obtained workflow failure with missing report.
- Independently useful ports avoid restoring retired fabric_saga coupling. A small consumer mapping keeps package vocabularies explicit.
- Safe summaries omit payloads and preserve all settlement categories; caller explain chooses disclosure separately.
- Retiring receiver on synchronous return would lose abnormal owner-exit settlement. A long-lived owner retains receivers and is outside intended use.

## Evidence and history

- Ports: `f280539d2bc7f6264f498dea2691cb2387c514f9`, 2026-10-05. Bounded startup failure: `299e2b9c032b50e2fd2260c7aac97c51189545d8`.
- Full reports/summary: `29e9e9a4ff9c65964e01641d590f8760af41cae3`; documentation alignment: `2a6bc4bf380145e62015e66e15d216f9a6d7d1d4`.
- Evidence: outcome/reporting/startup source/tests, native reporting consumer, Fabric saga_tool recipe. Owner rationale: Oversight DECISIONS pre-release composition and PLAN Round 9 typed-composition follow-up. Earlier flattened-report examples are superseded.
