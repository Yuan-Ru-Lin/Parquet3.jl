# Read benchmark on testdata/part-0.parquet (a local file, not in the repository).
# Not part of the test suite. Procedure and reference numbers: dev-note.md, "Benchmark gate".
#
#     julia --project=. -t4 test/benchmark_part0.jl
using Parquet3

const PATH = joinpath(@__DIR__, "..", "testdata", "part-0.parquet")
isfile(PATH) || error("benchmark file not found: $PATH")
read_parquet(PATH)      # compile

for columns in (nothing, ["waveform_windowed"], ["waveform_presummed"], ["tracelist"])
    runs = [(@timed read_parquet(PATH; columns)) for _ in 1:15]
    times = sort([r.time for r in runs]) .* 1000
    @info "$(columns === nothing ? "all columns" : only(columns))" min_ms = round(times[1], digits = 1) median_ms = round(times[8], digits = 1) alloc_MiB = round(minimum(r.bytes for r in runs) / 2^20, digits = 1)
end
@info "threads" n = Threads.nthreads()
