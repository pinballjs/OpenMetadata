package org.openmetadata.catalog.resources.operations;

import org.apache.http.client.HttpResponseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.openmetadata.catalog.CatalogApplicationTest;
import org.openmetadata.catalog.api.operations.CreateDataLakeBucket;
import org.openmetadata.catalog.api.operations.CreateStarburstTable;
import org.openmetadata.catalog.entity.data.Location;
import org.openmetadata.catalog.entity.data.Table;
import org.openmetadata.catalog.operations.TableFormat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.openmetadata.catalog.util.TestUtils.assertResponse;
import static org.openmetadata.catalog.util.TestUtils.authHeaders;
import static org.openmetadata.catalog.util.TestUtils.post;
import static org.openmetadata.catalog.util.TestUtils.userAuthHeaders;

public class OperationResourceTest extends CatalogApplicationTest {
    @Test
    public void post_tableAlreadyExists_201_works(TestInfo test) throws HttpResponseException {
        CreateStarburstTable create = create(test, "cluster1.catalog1.schema1.table1", "s3://bucket/dwh/schema/table");
        createStarburstTable(create, userAuthHeaders());
        createStarburstTable(create, userAuthHeaders());
    }

    @Test
    public void post_tableForbiddenLocation_403(TestInfo test) throws HttpResponseException {
        createBucket(test, "s3://bucket", "test2@open-metadata.org");
        CreateStarburstTable create = create(test, "cluster1.catalog1.schema1.table1", "s3://bucket/dwh/schema/table");
        HttpResponseException exception = assertThrows(HttpResponseException.class, () ->
                createStarburstTable(create, userAuthHeaders()));
        assertResponse(exception, FORBIDDEN, "Team: Alfred is not the owner of " + getServiceName(test) + "://bucket/dwh/schema/table");
    }

    public static String getServiceName(TestInfo test) {
        return String.format("s3_%s", test.getDisplayName());
    }

    public static String getServiceName(TestInfo test, int index) {
        return String.format("s3_%d_%s", index, test.getDisplayName());
    }

    private static void createBucket(TestInfo test, String location, String user) throws HttpResponseException {
        String service = getServiceName(test);
        List<String> locationParts = Arrays.asList(location.replaceAll(" ", "")
                .split("://"));
        CreateDataLakeBucket create = new CreateDataLakeBucket().withName(service + "://" + locationParts.get(1));
        post(getResource("operations/bucket"), create, Location.class, authHeaders(user));
    }

    private static CreateStarburstTable create(TestInfo test, String table, String location) {
        String service = getServiceName(test);
        List<String> locationParts = Arrays.asList(location.replaceAll(" ", "")
                .split("://"));
        return new CreateStarburstTable().withName(table).withLocation(service + "://" + locationParts.get(1))
                .withTableFormat(TableFormat.DELTA).withColumns(null);
    }

    public static Table createStarburstTable(CreateStarburstTable create,
                                          Map<String, String> authHeaders) throws HttpResponseException {
        return post(getResource("operations/table"), create, Table.class, authHeaders);
    }
}
