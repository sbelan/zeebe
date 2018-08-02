# Exporter (Broker)

## Notes

### Discarded ideas

1. Exporter config in shared file (either in `zeebe.cfg.toml` or another
`exporters.cfg.toml`).
    1. Potentially can be long, plus we force operators
    to inject configuration the way we want.
    1. Admittedly, we already force them to use TOML, so...
    1. Not as clean, either do two configuration parsing, or pass down
    unparsed document fragment.
1.

### Unsure ideas

1. One exporter per JAR vs multiple exporters per JAR
    1. One exporter is less flexible, fat JARs, more projects (or more
    complicated configurations building multiple targets for the same
    project).
    1. Simpler, though, and you can specify your exporter as the main
    class. Not sure if it's worth it. And
    1. One exporter also means upgrading your exporters simpler, less
    dangerous (smaller blast radius) since (potentially) upgrading an
    exporter means just removing its previous versions, versus having
    to stop/unload all from the same JAR just to upgrade a single one.
    1. Multiple exporters allows you to have a single project, reuse
    code, share dependencies, etc. Does complicate things though...

### Goals

1. Streamline production of exporters: writing an exporter should focus
entirely on exporting a set of records to an external source, and
nothing else - no fiddling with scheduling, service containers, etc.
Should be as simple as possible.
1. Stick to common/known Java.
1. Support asynchronous operations.
1. Provide standardized facilities for common operations (e.g. Logging)
1. Provide test harness, again to streamline production of exporters,
i.e. make it as easy to test as possible. Questions: is an embedded
broker necessary? Could we provide some basic sanity tests?
1. Internal configuration of exporter is responsibility of exporter
developer (i.e. whether JSON, TOML, Properties, env vars, etc.).
1. Should be easy to deploy/operate: a single JAR with many exporters
might be the simplest here.
    1. Allows code reuse (i.e. configuration), shared dependencies
    1. Let operator decide how internal configuration is injected.

### Error handling

1. How is error handling going to look like? Ideally similar to
everything else. Incidents? Each exporter will run in a stream
processor, so why not?

## Configuration

A new section is added to the `BrokerCfg` (see: `ExporterCfg`), which
allows operators to configure:

1. `path`: Location of exporters (default: `<broker-root>/exporters`)
1. `enabled`: List of exporter IDs to load (with special value: `*` to
load all)

## Filesystem/layout

A configurable folder (default: `exporters` relative to broker root)
will server as a repository for all exporters.

Inside will be exporter bundles, which are essentially just folders
with a specific structure.

A bundle consists of:
1. A `lib` folder containing all JARs to load.
1. A `resources` folder which will be part of the classpath (e.g. read
injected configuration file)

## Bundle

An `ExporterBundle` has a unique ID (the URI to the its folder),
a class loader (which can load from all JARs in `lib`, system classes,
and any `io.zeebe.exporter.*` classes).

Bundles are loaded on startup by the `ExporterComponent`, which handles
loading/unloading of bundles, collisions, etc.

## Exporters

Exporters are marked by users using the `@Exporter` annotation, which
has a single value, the exporter's ID (which must be unique).
