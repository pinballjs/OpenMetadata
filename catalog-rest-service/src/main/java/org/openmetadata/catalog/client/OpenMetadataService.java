/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.openmetadata.catalog.client;

import org.apache.http.client.HttpResponseException;
import org.eclipse.jetty.http.HttpStatus;
import org.openmetadata.catalog.api.data.CreateDatabase;
import org.openmetadata.catalog.api.data.CreateLocation;
import org.openmetadata.catalog.api.data.CreateTable;
import org.openmetadata.catalog.api.services.CreateDatabaseService;
import org.openmetadata.catalog.api.services.CreateStorageService;
import org.openmetadata.catalog.api.teams.CreateTeam;
import org.openmetadata.catalog.api.teams.CreateUser;
import org.openmetadata.catalog.entity.data.Database;
import org.openmetadata.catalog.entity.data.Location;
import org.openmetadata.catalog.entity.data.Table;
import org.openmetadata.catalog.entity.services.DatabaseService;
import org.openmetadata.catalog.entity.services.StorageService;
import org.openmetadata.catalog.entity.teams.Team;
import org.openmetadata.catalog.entity.teams.User;
import org.openmetadata.catalog.resources.locations.LocationResource.LocationList;
import org.openmetadata.catalog.security.CatalogOpenIdAuthorizationRequestFilter;
import org.openmetadata.catalog.type.EntityReference;
import org.openmetadata.catalog.util.EntityUtil;

import javax.json.JsonObject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;

public class OpenMetadataService {

    public static int port;
    private static Client client;

    public OpenMetadataService(Client client) {
        OpenMetadataService.client = client;
    }

    public static Database createDatabase(CreateDatabase create, Map<String, String> authHeaders) throws HttpResponseException {
        String databaseFQN = create.getService().getName() + "." + create.getName();
        if (existsDatabaseByName(databaseFQN, authHeaders)) {
            return getDatabaseByName(databaseFQN, "owner", authHeaders);
        } else {
            return post(getResource("databases"), create, Database.class, authHeaders);
        }
    }

    private static Database getDatabaseByName(String name, String fields, Map<String, String> authHeaders) throws HttpResponseException {
        WebTarget target = getResource("databases/name/" + name);
        target = fields != null ? target.queryParam("fields", fields) : target;
        return get(target, Database.class, authHeaders);
    }

    private static boolean existsDatabaseByName(String name, Map<String, String> authHeaders) throws HttpResponseException {
        boolean found = false;
        try {
            getDatabaseByName(name, null, authHeaders);
            found = true;
        } catch(HttpResponseException ex) {
            if (ex.getStatusCode() != NOT_FOUND.getStatusCode()) {
                throw ex;
            }
        }
        return found;
    }

    public static DatabaseService createDatabaseService(CreateDatabaseService create, Map<String, String> authHeaders) throws HttpResponseException {
        if (existsDatabaseServiceByName(create.getName(), authHeaders)) {
            return getDatabaseServiceByName(create.getName(), "owner", authHeaders);
        } else {
            return post(getResource("services/databaseServices"), create, DatabaseService.class, authHeaders);
        }
    }

    public static boolean existsDatabaseServiceByName(String name, Map<String, String> authHeaders) throws HttpResponseException {
        boolean found = false;
        try {
            getDatabaseServiceByName(name, null, authHeaders);
            found = true;
        } catch(HttpResponseException ex) {
            if (ex.getStatusCode() != NOT_FOUND.getStatusCode()) {
                throw ex;
            }
        }
        return found;
    }

    public static DatabaseService getDatabaseServiceByName(String name, String fields, Map<String, String> authHeaders)
            throws HttpResponseException {
        WebTarget target = getResource("services/databaseServices/name/" + name);
        target = fields != null ? target.queryParam("fields", fields) : target;
        return get(target, DatabaseService.class, authHeaders);
    }

    public static <T> T get(WebTarget target, Class<T> clz, Map<String, String> headers) throws HttpResponseException {
        final Response response = addHeaders(target, headers).get();
        return readResponse(response, clz, Response.Status.OK.getStatusCode());
    }


    public static <T, K> T post(WebTarget target, K request, Class<T> clz, Map<String, String> headers)
            throws HttpResponseException {
        Response response = addHeaders(target, headers).post(Entity.entity(request, MediaType.APPLICATION_JSON));
        return readResponse(response, clz, Response.Status.CREATED.getStatusCode());
    }

    public static Invocation.Builder addHeaders(WebTarget target, Map<String, String> headers) {
        if (headers != null) {
            return target.request().header(CatalogOpenIdAuthorizationRequestFilter.X_AUTH_PARAMS_EMAIL_HEADER,
                    headers.get(CatalogOpenIdAuthorizationRequestFilter.X_AUTH_PARAMS_EMAIL_HEADER));
        }
        return target.request();
    }

    public static <T> T readResponse(Response response, Class<T> clz, int expectedResponse) throws HttpResponseException {
        if (!HttpStatus.isSuccess(response.getStatus())) {
            readResponseError(response);
        }
        if (expectedResponse != response.getStatus()) {
            throw new HttpResponseException(response.getStatus(),
                    String.format("Status code expected (%s) is different from the status code received (%s",
                            expectedResponse, response.getStatus()));
        }
        return response.readEntity(clz);
    }

    public static void readResponseError(Response response) throws HttpResponseException {
        JsonObject error = response.readEntity(JsonObject.class);
        throw new HttpResponseException(error.getInt("code"), error.getString("message"));
    }

    public static WebTarget getResource(String collection) {
        String targetURI = "http://localhost:" + port + "/api/v1/" + collection;
        return client.target(targetURI);
    }

    public static Map<String, String> adminAuthHeaders() {
        return authHeaders("admin@open-metadata.org");
    }

    public static Map<String, String> userAuthHeaders() {
        return authHeaders("test@open-metadata.org");
    }

    public static Map<String, String> authHeaders(String username) {
        Map<String, String> headers = new HashMap<>();
        if (username != null) {
            headers.put(CatalogOpenIdAuthorizationRequestFilter.X_AUTH_PARAMS_EMAIL_HEADER, username);
        }
        return headers;
    }


    public static Database getDatabase(UUID id, Map<String, String> authHeaders) throws HttpResponseException {
        return getDatabase(id, null, authHeaders);
    }

    public static Database getDatabase(UUID id, String fields, Map<String, String> authHeaders)
            throws HttpResponseException {
        WebTarget target = getResource("databases/" + id);
        target = fields != null ? target.queryParam("fields", fields): target;
        return get(target, Database.class, authHeaders);
    }

    public static Table createTable(CreateTable create, Map<String, String> authHeaders) throws HttpResponseException {
        Database database = getDatabase(create.getDatabase(), authHeaders);
        String name = database.getFullyQualifiedName() + "." + create.getName();
        if (existsTableByName(name, authHeaders)) {
            return getTableByName(name, "location,owner", authHeaders);
        } else {
            return post(getResource("tables"), create, Table.class, authHeaders);
        }
    }

    private static Table getTableByName(String name, String fields, Map<String, String> authHeaders) throws HttpResponseException {
        WebTarget target = getResource("tables/name/" + name);
        target = fields != null ? target.queryParam("fields", fields) : target;
        return get(target, Table.class, authHeaders);
    }

    private static boolean existsTableByName(String name, Map<String, String> authHeaders)
            throws HttpResponseException {
        boolean found = false;
        try {
            getTableByName(name, null, authHeaders);
            found = true;
        } catch(HttpResponseException ex) {
            if (ex.getStatusCode() != NOT_FOUND.getStatusCode()) {
                throw ex;
            }
        }
        return found;
    }

    public static StorageService createStorageService(CreateStorageService create, Map<String, String> authHeaders)
            throws HttpResponseException {
        if (existsStorageServiceByName(create.getName(), authHeaders)) {
            return getStorageServiceByName(create.getName(), null, authHeaders);
        } else {
            return post(getResource("services/storageServices"), create, StorageService.class, authHeaders);
        }
    }

    private static StorageService getStorageServiceByName(String name, String fields, Map<String, String> authHeaders) throws HttpResponseException {
        WebTarget target = getResource("services/storageServices/name/" + name);
        target = fields != null ? target.queryParam("fields", fields) : target;
        return get(target, StorageService.class, authHeaders);
    }

    private static boolean existsStorageServiceByName(String name, Map<String, String> authHeaders) throws HttpResponseException {
        boolean found = false;
        try {
            getStorageServiceByName(name, null, authHeaders);
            found = true;
        } catch(HttpResponseException ex) {
            if (ex.getStatusCode() != NOT_FOUND.getStatusCode()) {
                throw ex;
            }
        }
        return found;
    }

    public static Location createLocation(CreateLocation create, Map<String, String> authHeaders) throws HttpResponseException {
        String encodedFqn = URLEncoder.encode(create.getService().getName() + ":/" + create.getName(), StandardCharsets.UTF_8);
        if (existsLocationByName(encodedFqn, authHeaders)) {
            return getLocationByName(encodedFqn, "owner", authHeaders);
        } else {
            return post(getResource("locations"), create, Location.class, authHeaders);
        }
    }

    public static Location getLocationByName(String name, String fields, Map<String, String> authHeaders) throws HttpResponseException {
        WebTarget target = getResource("locations/name/" + name);
        target = fields != null ? target.queryParam("fields", fields) : target;
        return get(target, Location.class, authHeaders);
    }

    private static boolean existsLocationByName(String name, Map<String, String> authHeaders) throws HttpResponseException {
        boolean found = false;
        try {
            getLocationByName(name, null, authHeaders);
            found = true;
        } catch(HttpResponseException ex) {
            if (ex.getStatusCode() != NOT_FOUND.getStatusCode()) {
                throw ex;
            }
        }
        return found;
    }

    public static void addLocationToTable(Location location, Table table, Map<String, String> authHeaders) throws HttpResponseException {
        WebTarget target = getResource(String.format("tables/%s/location", table.getId()));
        put(target, location.getId().toString(), Table.class, authHeaders);
    }

    public static <T, K> T put(WebTarget target, K request, Class<T> clz, Map<String, String> headers)
            throws HttpResponseException {
        Response response = addHeaders(target, headers).method("PUT", Entity.entity(request,
                MediaType.APPLICATION_JSON));
        return readResponse(response, clz, Response.Status.OK.getStatusCode());
    }

    public static User createUser(CreateUser create, Map<String, String> authHeaders) throws HttpResponseException {
            if (existsUserByName(create.getName(), authHeaders)) {
                return getUserByName(create.getName(), null, authHeaders);
            } else {
                return post(getResource("users"), create, User.class, authHeaders);
            }
        }

    private static boolean existsUserByName(String name, Map<String, String> authHeaders) throws HttpResponseException {
        boolean found = false;
        try {
            getUserByName(name, null, authHeaders);
            found = true;
        } catch(HttpResponseException ex) {
            if (ex.getStatusCode() != NOT_FOUND.getStatusCode()) {
                throw ex;
            }
        }
        return found;
    }

    private static User getUserByName(String name, String fields, Map<String, String> authHeaders) throws HttpResponseException {
        WebTarget target = getResource("users/name/" + name);
        target = fields != null ? target.queryParam("fields", fields) : target;
        return get(target, User.class, authHeaders);
    }

    public static Team createTeam(CreateTeam create, Map<String, String> authHeaders) throws HttpResponseException {
        List<UUID> users = create.getUsers();
        Team team;
        if (existsTeamByName(create.getName(), authHeaders)) {
            team = getTeamByName(create.getName(), "users", authHeaders);
        } else {
            team = post(getResource("teams"), create, Team.class, authHeaders);
        }
        addUsersToTeam(users, team, authHeaders);
        return team;
    }

    private static void addUsersToTeam(List<UUID> users, Team team, Map<String, String> authHeaders) throws HttpResponseException {
        for (UUID id: users) {
            User user = getUser(id, "teams", authHeaders);
            List<EntityReference> teams = Optional.ofNullable(user.getTeams()).orElse(Collections.emptyList());
            teams.add(EntityUtil.getEntityReference(team));
            WebTarget target = getResource(String.format("users", id));
            CreateUser createUser = new CreateUser().withEmail(user.getEmail()).withName(user.getName())
                    .withTeams(teams.stream().map(e -> e.getId()).distinct().collect(Collectors.toList()));
            put(target, createUser, User.class, authHeaders);
        }
    }

    private static User getUser(UUID id, String fields, Map<String, String> authHeaders) throws HttpResponseException {
        WebTarget target = getResource("users/" + id);
        target = fields != null ? target.queryParam("fields", fields) : target;
        return get(target, User.class, authHeaders);
    }

    private static Team getTeamByName(String name, String fields, Map<String, String> authHeaders) throws HttpResponseException {
        WebTarget target = getResource("teams/name/" + name);
        target = fields != null ? target.queryParam("fields", fields) : target;
        return get(target, Team.class, authHeaders);
    }

    private static boolean existsTeamByName(String name, Map<String, String> authHeaders) throws HttpResponseException {
        boolean found = false;
        try {
            getTeamByName(name, null, authHeaders);
            found = true;
        } catch(HttpResponseException ex) {
            if (ex.getStatusCode() != NOT_FOUND.getStatusCode()) {
                throw ex;
            }
        }
        return found;
    }

    public static LocationList getLocationsByPrefix(String fields, String fqnPrefix, String limit, String before, String after, Map<String, String> authHeaders) throws HttpResponseException {
        WebTarget target = getResource("locations");
        target = fields != null ? target.queryParam("fields", fields) : target;
        target = fqnPrefix!= null ? target.queryParam("fqnPrefix", fqnPrefix) : target;
        target = limit != null ? target.queryParam("limit", limit) : target;
        target = before != null ? target.queryParam("before", before) : target;
        target = after != null ? target.queryParam("after", after) : target;
        return get(target, LocationList.class, authHeaders);
    }
}