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

package org.openmetadata.catalog.resources.operations.table;

import com.google.inject.Inject;
import io.swagger.annotations.Api;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.apache.http.client.HttpResponseException;
import org.openmetadata.catalog.api.data.CreateDatabase;
import org.openmetadata.catalog.api.data.CreateLocation;
import org.openmetadata.catalog.api.data.CreateTable;
import org.openmetadata.catalog.api.operations.CreateStarburstTable;
import org.openmetadata.catalog.api.services.CreateDatabaseService;
import org.openmetadata.catalog.api.services.CreateStorageService;
import org.openmetadata.catalog.api.services.CreateStorageService.StorageServiceType;
import org.openmetadata.catalog.api.teams.CreateTeam;
import org.openmetadata.catalog.api.teams.CreateUser;
import org.openmetadata.catalog.client.OpenMetadataService;
import org.openmetadata.catalog.entity.data.Database;
import org.openmetadata.catalog.entity.data.Location;
import org.openmetadata.catalog.entity.data.Table;
import org.openmetadata.catalog.entity.services.DatabaseService;
import org.openmetadata.catalog.entity.services.StorageService;
import org.openmetadata.catalog.entity.teams.Team;
import org.openmetadata.catalog.entity.teams.User;
import org.openmetadata.catalog.jdbi3.CollectionDAO;
import org.openmetadata.catalog.resources.Collection;
import org.openmetadata.catalog.resources.locations.LocationResource.LocationList;
import org.openmetadata.catalog.security.AuthenticationException;
import org.openmetadata.catalog.security.AuthorizationException;
import org.openmetadata.catalog.security.CatalogAuthorizer;
import org.openmetadata.catalog.type.EntityReference;
import org.openmetadata.catalog.type.JdbcInfo;
import org.openmetadata.catalog.type.LocationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.openmetadata.catalog.Entity.DATABASE_SERVICE;
import static org.openmetadata.catalog.Entity.STORAGE_SERVICE;
import static org.openmetadata.catalog.api.services.CreateDatabaseService.DatabaseServiceType.Presto;
import static org.openmetadata.catalog.client.OpenMetadataService.adminAuthHeaders;

@Path("/v1/operations/table")
@Api(value = "Operations collection", tags = "Operations collection")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Collection(name = "operations")
public class TableResource {
    private static final Logger LOG = LoggerFactory.getLogger(TableResource.class);
    private final CollectionDAO dao;
    private final CatalogAuthorizer authorizer;

    @Inject
    public TableResource(CollectionDAO dao, CatalogAuthorizer authorizer) {
        Objects.requireNonNull(dao, "OperationRepository must not be null");
        this.dao = dao;
        this.authorizer = authorizer;
    }

    @POST
    @Operation(summary = "Create a table", tags = "operations",
            description = "Create a table",
            responses = {
                    @ApiResponse(responseCode = "200", description = "The table",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = Table.class))),
                    @ApiResponse(responseCode = "400", description = "Bad request")
            })
    public Response createIfNotExists(@Context UriInfo uriInfo, @Context SecurityContext securityContext,
                           @Valid CreateStarburstTable create) throws IOException {
        Principal principal = securityContext.getUserPrincipal();
        if (principal == null) {
            throw new AuthenticationException("No principal in AuthenticationContext");
        }
        CreateUser createUser = new CreateUser().withName(principal.getName())
                .withEmail(principal.getName() + "@open-metadata.org");
        User user = OpenMetadataService.createUser(createUser, adminAuthHeaders());
        // findOrCreate team
        // Integration with the Company Directory to find the team of the principal.
        String teamName;
        if (principal.getName().equals("test2")) {
            teamName = "Styx";
        } else {
            teamName = "Alfred";
        }
        CreateTeam createTeam = new CreateTeam().withName(teamName).withUsers(Arrays.asList(user.getId()));
        Team team = OpenMetadataService.createTeam(createTeam, adminAuthHeaders());
        EntityReference owner = new EntityReference().withId(team.getId()).withType("team");
        // create if not exists storageService
        List<String> locationParts = Arrays.asList(create.getLocation().split("://"));
        checkCanCreateLocation(teamName, create.getLocation());
        CreateStorageService createStorageService = new CreateStorageService().withServiceType(StorageServiceType.S3)
                .withName(locationParts.get(0));
        StorageService storageService = OpenMetadataService.createStorageService(createStorageService, adminAuthHeaders());
        // create if not exists location
        EntityReference storageServiceRef = new EntityReference().withId(storageService.getId())
                .withName(storageService.getName()).withDescription(storageService.getDescription())
                .withDisplayName(storageService.getDisplayName()).withType(STORAGE_SERVICE);
        CreateLocation createLocation = new CreateLocation().withService(storageServiceRef)
                .withLocationType(LocationType.Table).withName("/" + locationParts.get(1)).withOwner(owner);
        Location location = OpenMetadataService.createLocation(createLocation, adminAuthHeaders());
        // create if not exists databaseService
        List<String> fqnTableName = Arrays.asList(create.getName().replaceAll(" ", "")
                .split("\\."));
        CreateDatabaseService createDatabaseService = new CreateDatabaseService()
                .withName(String.format("%s_%s", fqnTableName.get(0), fqnTableName.get(1))).withServiceType(Presto)
                .withJdbc(new JdbcInfo().withConnectionUrl("c").withDriverClass("d"));
        DatabaseService databaseService = OpenMetadataService.createDatabaseService(createDatabaseService, adminAuthHeaders());
        EntityReference databaseServiceRef = new EntityReference().withId(databaseService.getId())
                .withName(databaseService.getName()).withDescription(databaseService.getDescription())
                .withDisplayName(databaseService.getDisplayName()).withType(DATABASE_SERVICE);
        // create if not exists database
        CreateDatabase createDatabase = new CreateDatabase().withName(fqnTableName.get(2))
                .withService(databaseServiceRef).withOwner(owner);
        Database database = OpenMetadataService.createDatabase(createDatabase, adminAuthHeaders());
        // create if not exists table
        CreateTable createTable = new CreateTable().withDatabase(database.getId()).withName(fqnTableName.get(3))
                .withColumns(new ArrayList<>()).withOwner(owner);
        Table table = OpenMetadataService.createTable(createTable, adminAuthHeaders());
        // create if not exists location
        if (table.getLocation() == null) {
            // add location to table
            OpenMetadataService.addLocationToTable(location, table, adminAuthHeaders());
        }
        // add parent location to database if null
        return Response.created(table.getHref()).entity(table).build();
    }

    public static void checkCanCreateLocation(String teamName, String location) throws HttpResponseException {
        List<String> locationParts = Arrays.asList((location.split("://")));
        String prefix = locationParts.get(0) + "://" + locationParts.get(1).split("/")[0];
        LocationList locations = OpenMetadataService.getLocationsByPrefix("owner", prefix, null, null, null, adminAuthHeaders());
        List<String> owners = locations.getData().stream().map(l -> {
            // check location is upstream folder otherwise null.
            LOG.info(l.getFullyQualifiedName());
            LOG.info(l.getOwner().toString());
            if (l.getOwner() != null) {
                return l.getOwner().getName();
            } else {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
        if (!owners.contains(teamName) && !owners.isEmpty()) {
            throw new AuthorizationException("Team: " + teamName + " is not the owner of " + location);
        }
    }
}