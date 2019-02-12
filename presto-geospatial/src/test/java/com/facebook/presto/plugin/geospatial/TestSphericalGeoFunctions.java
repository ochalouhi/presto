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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestSphericalGeoFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    protected void registerFunctions()
    {
        GeoPlugin plugin = new GeoPlugin();
        for (Type type : plugin.getTypes()) {
            functionAssertions.getTypeRegistry().addType(type);
        }
        functionAssertions.getMetadata().addFunctions(extractFunctions(plugin.getFunctions()));
    }

    @Test
    public void testGetObjectValue()
    {
        List<String> wktList = ImmutableList.of(
                "POINT EMPTY",
                "MULTIPOINT EMPTY",
                "LINESTRING EMPTY",
                "MULTILINESTRING EMPTY",
                "POLYGON EMPTY",
                "MULTIPOLYGON EMPTY",
                "GEOMETRYCOLLECTION EMPTY",
                "POINT (-40.2 28.9)",
                "MULTIPOINT ((-40.2 28.9), (-40.2 31.9))",
                "LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9)",
                "MULTILINESTRING ((-40.2 28.9, -40.2 31.9), (-40.2 31.9, -37.2 31.9))",
                "POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9))",
                "POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9), (-39.2 29.9, -39.2 30.9, -38.2 30.9, -38.2 29.9, -39.2 29.9))",
                "MULTIPOLYGON (((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)), ((-39.2 29.9, -38.2 29.9, -38.2 30.9, -39.2 30.9, -39.2 29.9)))",
                "GEOMETRYCOLLECTION (POINT (-40.2 28.9), LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9), POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)))");

        BlockBuilder builder = SPHERICAL_GEOGRAPHY.createBlockBuilder(null, wktList.size());
        for (String wkt : wktList) {
            SPHERICAL_GEOGRAPHY.writeSlice(builder, GeoFunctions.toSphericalGeography(GeoFunctions.stGeometryFromText(utf8Slice(wkt))));
        }
        Block block = builder.build();
        for (int i = 0; i < wktList.size(); i++) {
            assertEquals(wktList.get(i), SPHERICAL_GEOGRAPHY.getObjectValue(null, block, i));
        }
    }

    @Test
    public void testToAndFromSphericalGeography()
    {
        // empty geometries
        assertToAndFromSphericalGeography("POINT EMPTY");
        assertToAndFromSphericalGeography("MULTIPOINT EMPTY");
        assertToAndFromSphericalGeography("LINESTRING EMPTY");
        assertToAndFromSphericalGeography("MULTILINESTRING EMPTY");
        assertToAndFromSphericalGeography("POLYGON EMPTY");
        assertToAndFromSphericalGeography("MULTIPOLYGON EMPTY");
        assertToAndFromSphericalGeography("GEOMETRYCOLLECTION EMPTY");

        // valid nonempty geometries
        assertToAndFromSphericalGeography("POINT (-40.2 28.9)");
        assertToAndFromSphericalGeography("MULTIPOINT ((-40.2 28.9), (-40.2 31.9))");
        assertToAndFromSphericalGeography("LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9)");
        assertToAndFromSphericalGeography("MULTILINESTRING ((-40.2 28.9, -40.2 31.9), (-40.2 31.9, -37.2 31.9))");
        assertToAndFromSphericalGeography("POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9))");
        assertToAndFromSphericalGeography("POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9), " +
                "(-39.2 29.9, -39.2 30.9, -38.2 30.9, -38.2 29.9, -39.2 29.9))");
        assertToAndFromSphericalGeography("MULTIPOLYGON (((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)), " +
                "((-39.2 29.9, -38.2 29.9, -38.2 30.9, -39.2 30.9, -39.2 29.9)))");
        assertToAndFromSphericalGeography("GEOMETRYCOLLECTION (POINT (-40.2 28.9), LINESTRING (-40.2 28.9, -40.2 31.9, -37.2 31.9), " +
                "POLYGON ((-40.2 28.9, -37.2 28.9, -37.2 31.9, -40.2 31.9, -40.2 28.9)))");

        // geometries containing invalid latitude or longitude values
        assertInvalidLongitude("POINT (-340.2 28.9)");
        assertInvalidLatitude("MULTIPOINT ((-40.2 128.9), (-40.2 31.9))");
        assertInvalidLongitude("LINESTRING (-40.2 28.9, -40.2 31.9, 237.2 31.9)");
        assertInvalidLatitude("MULTILINESTRING ((-40.2 28.9, -40.2 31.9), (-40.2 131.9, -37.2 31.9))");
        assertInvalidLongitude("POLYGON ((-40.2 28.9, -40.2 31.9, 237.2 31.9, -37.2 28.9, -40.2 28.9))");
        assertInvalidLatitude("POLYGON ((-40.2 28.9, -40.2 31.9, -37.2 131.9, -37.2 28.9, -40.2 28.9), (-39.2 29.9, -39.2 30.9, -38.2 30.9, -38.2 29.9, -39.2 29.9))");
        assertInvalidLongitude("MULTIPOLYGON (((-40.2 28.9, -40.2 31.9, -37.2 31.9, -37.2 28.9, -40.2 28.9)), " +
                "((-39.2 29.9, -39.2 30.9, 238.2 30.9, -38.2 29.9, -39.2 29.9)))");
        assertInvalidLatitude("GEOMETRYCOLLECTION (POINT (-40.2 28.9), LINESTRING (-40.2 28.9, -40.2 131.9, -37.2 31.9), " +
                "POLYGON ((-40.2 28.9, -40.2 31.9, -37.2 31.9, -37.2 28.9, -40.2 28.9)))");
    }

    private void assertToAndFromSphericalGeography(String wkt)
    {
        assertFunction(format("ST_AsText(to_geometry(to_spherical_geography(ST_GeometryFromText('%s'))))", wkt), VARCHAR, wkt);
    }

    private void assertInvalidLongitude(String wkt)
    {
        assertInvalidFunction(format("to_spherical_geography(ST_GeometryFromText('%s'))", wkt), "Longitude must be between -180 and 180");
    }

    private void assertInvalidLatitude(String wkt)
    {
        assertInvalidFunction(format("to_spherical_geography(ST_GeometryFromText('%s'))", wkt), "Latitude must be between -90 and 90");
    }

    @Test
    public void testDistance()
    {
        assertDistance("POINT (-86.67 36.12)", "POINT (-118.40 33.94)", 2886448.973436703);
        assertDistance("POINT (-118.40 33.94)", "POINT (-86.67 36.12)", 2886448.973436703);
        assertDistance("POINT (-71.0589 42.3601)", "POINT (-71.2290 42.4430)", 16734.69743457461);
        assertDistance("POINT (-86.67 36.12)", "POINT (-86.67 36.12)", 0.0);

        assertDistance("POINT EMPTY", "POINT (40 30)", null);
        assertDistance("POINT (20 10)", "POINT EMPTY", null);
        assertDistance("POINT EMPTY", "POINT EMPTY", null);
    }

    private void assertDistance(String wkt, String otherWkt, Double expectedDistance)
    {
        assertFunction(format("ST_Distance(to_spherical_geography(ST_GeometryFromText('%s')), to_spherical_geography(ST_GeometryFromText('%s')))", wkt, otherWkt), DOUBLE, expectedDistance);
    }

    @Test
    public void testArea()
            throws IOException
    {
        //PA
        assertPolygonAreaWithinPrecision("-79.76278 42.252649, -79.76278 42.000709, -75.35932 42.000709, -75.249781 41.863786, -75.173104 41.869263, -75.052611 41.754247, -75.074519 41.60637, -74.89378 41.436584, -74.740426 41.431108, -74.69661 41.359907, -74.828057 41.288707, -74.882826 41.179168, -75.134765 40.971045, -75.052611 40.866983, -75.205966 40.691721, -75.195012 40.576705, -75.069042 40.543843, -75.058088 40.417874, -74.773287 40.215227, -74.82258 40.127596, -75.129289 39.963288, -75.145719 39.88661, -75.414089 39.804456, -75.616736 39.831841, -75.786521 39.722302, -79.477979 39.722302, -80.518598 39.722302, -80.518598 40.636951, -80.518598 41.978802, -80.518598 41.978802, -80.332382 42.033571, -79.76278 42.269079, -79.76278 42.252649", 117255.1312637E6, 2E2);

        //A polygon around the North Pole
        assertPolygonAreaWithinPrecision("-135 85, -45 85, 45 85, 135 85, -135 85", 619002051089.943, 1E3);

        assertPolygonAreaWithinPrecision("0 0, 0 1, 1 1, 1 0", 12364E6, 1E3);

        assertPolygonAreaWithinPrecision("-122.150124 37.486095, -122.149201 37.486606,  -122.145725 37.486580, -122.145923 37.483961 , -122.149324 37.482480 ,  -122.150837 37.483238,  -122.150901 37.485392", 163290.93943446054, 1E5);
        double angleOfOneKm = 0.008993201943349;
        assertPolygonAreaWithinPrecision(format("0 0, %.15f 0, %.15f %.15f, 0 %.15f", angleOfOneKm, angleOfOneKm, angleOfOneKm, angleOfOneKm), 1E6, 1E5);

        //1/4th of an hemisphere, ie 1/8th of the planet, should be close to 4PiR2/8 = 637.5E11
        assertPolygonAreaWithinPrecision("90 0, 0 0, 0 90", 637.5E11, 1E3);

        Path geometryPath = Paths.get(BenchmarkGeometryAggregations.class.getClassLoader().getResource("us-states.tsv").getPath());
        Map<String, String> stateGeometries = Files.lines(geometryPath)
                .map(line -> line.split("\t"))
                .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));

        Path areaPath = Paths.get(BenchmarkGeometryAggregations.class.getClassLoader().getResource("us-states-area.tsv").getPath());
        Map<String, Double> stateAreas = Files.lines(areaPath)
                .map(line -> line.split("\t"))
                .filter(parts -> parts.length >= 2)
                .collect(Collectors.toMap(parts -> parts[0], parts -> Double.valueOf(parts[1]) * 1E6));

        for (String state : stateGeometries.keySet()) {
            Double stateArea = stateAreas.get(state);
            String stateGeometry = stateGeometries.get(state);
            if (stateArea != null && stateGeometry != null) {
                assertWKTAreaWithinPrecision(stateGeometry, stateArea, 200); // 0.5% max difference
            }
        }
    }

    private void assertPolygonArea(String polygonWkt, double expectedArea)
    {
        assertWKTArea(format("POLYGON((%s))", polygonWkt), expectedArea);
    }

    private void assertPolygonAreaWithinPrecision(String polygonWkt, double expectedArea, double precision)
    {
        assertWKTAreaWithinPrecision(format("POLYGON((%s))", polygonWkt), expectedArea, precision);
    }

    private void assertWKTArea(String wkt, double expectedArea)
    {
        assertFunction(format("ST_Area(to_spherical_geography(ST_GeometryFromText('%s')))", wkt), DOUBLE, expectedArea);
    }

    private void assertWKTAreaWithinPrecision(String wkt, double expectedArea, double precision)
    {
        assertFunction(format("ABS(ROUND((ST_Area(to_spherical_geography(ST_GeometryFromText('%s'))) / %s - 1 ) * %s, 0))", wkt, expectedArea, precision), DOUBLE, 0.0);
    }
}
