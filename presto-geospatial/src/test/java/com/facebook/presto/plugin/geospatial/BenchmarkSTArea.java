/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.geospatial;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.plugin.geospatial.GeometryBenchmarkUtils.createPolygon;
import static com.facebook.presto.plugin.geospatial.GeometryBenchmarkUtils.loadPolygon;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSTArea
{
    @Benchmark
    public Object stSphericalArea(BenchmarkData data)
    {
        return GeoFunctions.stSphericalArea(data.geometry);
    }

    @Benchmark
    public Object stSphericalArea500k(BenchmarkData data)
    {
        return GeoFunctions.stSphericalArea(data.geometry500k);
    }

    @Benchmark
    public Object stSphericalAreaIgnorePoles(BenchmarkData data)
    {
        return GeoFunctions.stSphericalArea(data.geometry, true);
    }

    @Benchmark
    public Object stSphericalArea500kIgnorePoles(BenchmarkData data)
    {
        return GeoFunctions.stSphericalArea(data.geometry500k, true);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private Slice geometry;
        private Slice geometry500k;

        @Setup
        public void setup()
                throws IOException
        {
            geometry = GeoFunctions.toSphericalGeography(GeoFunctions.stGeometryFromText(Slices.utf8Slice(loadPolygon("large_polygon.txt"))));
            geometry500k = GeoFunctions.toSphericalGeography(GeoFunctions.stGeometryFromText(Slices.utf8Slice(createPolygon(9.0, 500000))));
        }
    }

    public static void main(String[] args)
            throws IOException, RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        BenchmarkSTArea benchmark = new BenchmarkSTArea();

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSTArea.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
