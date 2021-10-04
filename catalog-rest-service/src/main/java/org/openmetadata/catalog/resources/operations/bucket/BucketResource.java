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

package org.openmetadata.catalog.resources.operations.bucket;

import com.google.inject.Inject;
import io.swagger.annotations.Api;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.openmetadata.catalog.api.data.CreateLocation;
import org.openmetadata.catalog.api.operations.CreateDataLakeBucket;
import org.openmetadata.catalog.api.services.CreateStorageService;
import org.openmetadata.catalog.api.teams.CreateTeam;
import org.openmetadata.catalog.api.teams.CreateUser;
import org.openmetadata.catalog.client.OpenMetadataService;
import org.openmetadata.catalog.entity.data.Location;
import org.openmetadata.catalog.entity.services.StorageService;
import org.openmetadata.catalog.entity.teams.Team;
import org.openmetadata.catalog.entity.teams.User;
import org.openmetadata.catalog.jdbi3.OperationRepository;
import org.openmetadata.catalog.resources.Collection;
import org.openmetadata.catalog.security.AuthenticationException;
import org.openmetadata.catalog.security.CatalogAuthorizer;
import org.openmetadata.catalog.type.EntityReference;
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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.openmetadata.catalog.client.OpenMetadataService.adminAuthHeaders;
import static org.openmetadata.catalog.resources.operations.table.TableResource.checkCanCreateLocation;
import static org.openmetadata.catalog.util.EntityUtil.getEntityReference;

@Path("/v1/operations/bucket")
@Api(value = "Operations collection", tags = "Operations collection")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Collection(name = "operations", repositoryClass = "org.openmetadata.catalog.jdbi3.OperationRepository")
public class BucketResource {
    private static final Logger LOG = LoggerFactory.getLogger(BucketResource.class);
    private final OperationRepository dao;
    private final CatalogAuthorizer authorizer;

    @Inject
    public BucketResource(OperationRepository dao, CatalogAuthorizer authorizer) {
        Objects.requireNonNull(dao, "OperationRepository must not be null");
        this.dao = dao;
        this.authorizer = authorizer;
    }

    @POST
    @Operation(summary = "Create a bucket", tags = "operations",
            description = "Create a bucket",
            responses = {
                    @ApiResponse(responseCode = "200", description = "The bucket",
                            content = @Content(mediaType = "application/json",
                                    schema = @Schema(implementation = Location.class))),
                    @ApiResponse(responseCode = "400", description = "Bad request")
            })
    public Response createIfNotExists(@Context UriInfo uriInfo, @Context SecurityContext securityContext,
                                      @Valid CreateDataLakeBucket create) throws IOException {
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
        List<String> locationParts = Arrays.asList(create.getName().split("://"));
        checkCanCreateLocation(teamName, create.getName());
        CreateStorageService createStorageService = new CreateStorageService().withServiceType(CreateStorageService.StorageServiceType.S3)
                .withName(locationParts.get(0));
        StorageService storageService = OpenMetadataService.createStorageService(createStorageService, adminAuthHeaders());
        // create if not exists location
        CreateLocation createLocation = new CreateLocation().withService(getEntityReference(storageService))
                .withLocationType(LocationType.Bucket).withName("/" + locationParts.get(1)).withOwner(owner);
        Location location = OpenMetadataService.createLocation(createLocation, adminAuthHeaders());
        // create if not exists databaseService
        return Response.created(location.getHref()).entity(location).build();
    }
}