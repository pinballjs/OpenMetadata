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

package org.openmetadata.catalog.jdbi3;

import org.openmetadata.catalog.Entity;
import org.openmetadata.catalog.entity.data.Location;
import org.openmetadata.catalog.entity.services.StorageService;
import org.openmetadata.catalog.exception.CatalogExceptionMessage;
import org.openmetadata.catalog.exception.EntityNotFoundException;
import org.openmetadata.catalog.jdbi3.StorageServiceRepository.StorageServiceDAO;
import org.openmetadata.catalog.jdbi3.TeamRepository.TeamDAO;
import org.openmetadata.catalog.jdbi3.TagRepository.TagDAO;
import org.openmetadata.catalog.jdbi3.UserRepository.UserDAO;
import org.openmetadata.catalog.resources.locations.LocationResource;
import org.openmetadata.catalog.resources.locations.LocationResource.LocationList;
import org.openmetadata.catalog.type.EntityReference;
import org.openmetadata.catalog.type.TagLabel;
import org.openmetadata.catalog.util.EntityUtil;
import org.openmetadata.catalog.util.EntityUtil.Fields;
import org.openmetadata.catalog.util.JsonUtils;
import org.openmetadata.catalog.util.RestUtil.PutResponse;
import org.openmetadata.common.utils.CipherText;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.CreateSqlObject;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonPatch;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.openmetadata.catalog.exception.CatalogExceptionMessage.entityNotFound;

public abstract class LocationRepository {
    private static final Logger LOG = LoggerFactory.getLogger(LocationRepository.class);
    // Location fields that can be updated in a PUT request
    private static final Fields LOCATION_UPDATE_FIELDS = new Fields(LocationResource.FIELD_LIST, "owner,tags");
    // Location fields that can be patched in a PATCH request
    private static final Fields LOCATION_PATCH_FIELDS = new Fields(LocationResource.FIELD_LIST, "owner,service,tags");

    @CreateSqlObject
    abstract LocationDAO locationDAO();

    @CreateSqlObject
    abstract StorageServiceDAO storageServiceDAO();

    @CreateSqlObject
    abstract EntityRelationshipDAO relationshipDAO();

    @CreateSqlObject
    abstract UserDAO userDAO();

    @CreateSqlObject
    abstract TeamDAO teamDAO();

    @CreateSqlObject
    abstract TagDAO tagDAO();

    @Transaction
    public LocationList listAfter(Fields fields, String fqnPrefix, int limitParam, String after) throws IOException,
            GeneralSecurityException {
        // forward scrolling, if after == null then first page is being asked
        List<String> jsons = locationDAO().listAfter(fqnPrefix, limitParam + 1, after == null ? "" :
                CipherText.instance().decrypt(after));

        List<Location> locations = new ArrayList<>();
        for (String json : jsons) {
            locations.add(setFields(JsonUtils.readValue(json, Location.class), fields));
        }
        int total = locationDAO().listCount(fqnPrefix);

        String beforeCursor, afterCursor = null;
        beforeCursor = after == null ? null : locations.get(0).getFullyQualifiedName();
        if (locations.size() > limitParam) { // If extra result exists, then next page exists - return after cursor
            locations.remove(limitParam);
            afterCursor = locations.get(limitParam - 1).getFullyQualifiedName();
        }
        return new LocationList(locations, beforeCursor, afterCursor, total);
    }

    @Transaction
    public LocationList listBefore(Fields fields, String fqnPrefix, int limitParam, String before) throws IOException,
            GeneralSecurityException {
        // Reverse scrolling - Get one extra result used for computing before cursor
        List<String> jsons = locationDAO().listBefore(fqnPrefix, limitParam + 1, CipherText.instance().decrypt(before));
        List<Location> locations = new ArrayList<>();
        for (String json : jsons) {
            locations.add(setFields(JsonUtils.readValue(json, Location.class), fields));
        }
        int total = locationDAO().listCount(fqnPrefix);

        String beforeCursor = null, afterCursor;
        if (locations.size() > limitParam) { // If extra result exists, then previous page exists - return before cursor
            locations.remove(0);
            beforeCursor = locations.get(0).getFullyQualifiedName();
        }
        afterCursor = locations.get(locations.size() - 1).getFullyQualifiedName();
        return new LocationList(locations, beforeCursor, afterCursor, total);
    }

    @Transaction
    public Location get(String id, Fields fields) throws IOException {
        return setFields(validateLocation(id), fields);
    }

    public EntityReference getOwner(Location location) throws IOException {
        return location != null ? EntityUtil.populateOwner(location.getId(), relationshipDAO(), userDAO(), teamDAO()) : null;
    }

    private List<EntityReference> getFollowers(Location location) throws IOException {
        return location == null ? null : EntityUtil.getFollowers(location.getId(), relationshipDAO(), userDAO());
    }

    private Location validateLocation(String id) throws IOException {
        return EntityUtil.validate(id, locationDAO().findById(id), Location.class);
    }

    @Transaction
    public Location create(Location location, EntityReference service, EntityReference owner) throws IOException {
        getService(service); // Validate service
        return createInternal(location, service, owner);
    }

    private EntityReference getService(Location location) throws IOException {
        return location == null ? null : getService(Objects.requireNonNull(EntityUtil.getService(relationshipDAO(),
                location.getId())));
    }


    private EntityReference getService(EntityReference service) throws IOException {
        String id = service.getId().toString();
        if (service.getType().equalsIgnoreCase(Entity.STORAGE_SERVICE)) {
            StorageService serviceInstance = EntityUtil.validate(id, storageServiceDAO().findById(id), StorageService.class);
            service.setDescription(serviceInstance.getDescription());
            service.setName(serviceInstance.getName());
        } else {
            throw new IllegalArgumentException(String.format("Invalid service type %s for the storage", service.getType()));
        }
        return service;
    }

    private Location setFields(Location location, Fields fields) throws IOException {
        location.setOwner(fields.contains("owner") ? getOwner(location) : null);
        location.setService(fields.contains("service") ? getService(location) : null);
        location.setFollowers(fields.contains("followers") ? getFollowers(location) : null);
        location.setTags(fields.contains("tags") ? getTags(location.getFullyQualifiedName()) : null);
        return location;
    }

    private Location createInternal(Location location, EntityReference service, EntityReference owner)
            throws IOException {
        location.setFullyQualifiedName(getFQN(service, location));

        EntityUtil.populateOwner(userDAO(), teamDAO(), owner); // Validate owner

        locationDAO().insert(JsonUtils.pojoToJson(location));
        setService(location, service);
        setOwner(location, owner);
        applyTags(location);
        return location;
    }
    
    public void setService(Location location, EntityReference service) throws IOException {
        if (service != null && location != null) {
            getService(service); // Populate service details
            relationshipDAO().insert(service.getId().toString(), location.getId().toString(), service.getType(),
                    Entity.LOCATION, Relationship.CONTAINS.ordinal());
            location.setService(service);
        }
    }

    private void setOwner(Location location, EntityReference owner) {
        EntityUtil.setOwner(relationshipDAO(), location.getId(), Entity.LOCATION, owner);
        location.setOwner(owner);
    }
    
    private void applyTags(Location location) throws IOException {
        // Add location level tags by adding tag to location relationship
        EntityUtil.applyTags(tagDAO(), location.getTags(), location.getFullyQualifiedName());
        location.setTags(getTags(location.getFullyQualifiedName())); // Update tag to handle additional derived tags
    }

    private List<TagLabel> getTags(String fqn) {
        return tagDAO().getTags(fqn);
    }

    @Transaction
    public Location getByName(String fqn, Fields fields) throws IOException {
        Location location = EntityUtil.validate(fqn, locationDAO().findByFQN(fqn), Location.class);
        return setFields(location, fields);
    }

    @Transaction
    public void delete(String id) {
        if (locationDAO().delete(id) <= 0) {
            throw EntityNotFoundException.byMessage(entityNotFound(Entity.LOCATION, id));
        }
        // Remove all relationships
        relationshipDAO().deleteAll(id);
    }

    @Transaction
    public EntityReference getOwnerReference(Location location) throws IOException {
        return EntityUtil.populateOwner(userDAO(), teamDAO(), location.getOwner());
    }

    @Transaction
    public PutResponse<Location> createOrUpdate(Location updatedLocation, EntityReference service, EntityReference newOwner) throws
            IOException, ParseException {
        getService(service); // Validate service

        String fqn = getFQN(service, updatedLocation);
        Location storedDB = JsonUtils.readValue(locationDAO().findByFQN(fqn), Location.class);
        if (storedDB == null) {  // Location does not exist. Create a new one
            return new PutResponse<>(Response.Status.CREATED, createInternal(updatedLocation, service, newOwner));
        }
        // Update the existing location
        EntityUtil.populateOwner(userDAO(), teamDAO(), newOwner); // Validate new owner
        if (storedDB.getDescription() == null || storedDB.getDescription().isEmpty()) {
            storedDB.withDescription(updatedLocation.getDescription());
        }
        locationDAO().update(storedDB.getId().toString(), JsonUtils.pojoToJson(storedDB));

        // Update owner relationship
        setFields(storedDB, LOCATION_UPDATE_FIELDS); // First get the ownership information
        updateOwner(storedDB, storedDB.getOwner(), newOwner);

        // Service can't be changed in update since service name is part of FQN and
        // change to a different service will result in a different FQN and creation of a new location under the new service
        storedDB.setService(service);
        applyTags(updatedLocation);

        return new PutResponse<>(Response.Status.OK, storedDB);
    }

    private void updateOwner(Location location, EntityReference origOwner, EntityReference newOwner) {
        EntityUtil.updateOwner(relationshipDAO(), origOwner, newOwner, location.getId(), Entity.LOCATION);
        location.setOwner(newOwner);
    }

    public static String getFQN(EntityReference service, Location location) {
        return (service.getName() + ":/" + location.getName());
    }

    @Transaction
    public Response.Status addFollower(String locationId, String userId) throws IOException {
        EntityUtil.validate(locationId, locationDAO().findById(locationId), Location.class);
        return EntityUtil.addFollower(relationshipDAO(), userDAO(), locationId, Entity.LOCATION, userId, Entity.USER) ?
                Response.Status.CREATED : Response.Status.OK;
    }

    @Transaction
    public void deleteFollower(String locationId, String userId) {
        EntityUtil.validateUser(userDAO(), userId);
        EntityUtil.removeFollower(relationshipDAO(), locationId, userId);
    }

    @Transaction
    public Location patch(String id, JsonPatch patch) throws IOException {
        Location original = setFields(validateLocation(id), LOCATION_PATCH_FIELDS);
        Location updated = JsonUtils.applyPatch(original, patch, Location.class);
        patch(original, updated);
        return updated;
    }

    private void patch(Location original, Location updated) throws IOException {
        String locationId = original.getId().toString();
        if (!original.getId().equals(updated.getId())) {
            throw new IllegalArgumentException(CatalogExceptionMessage.readOnlyAttribute(Entity.LOCATION, "id"));
        }
        if (!original.getName().equals(updated.getName())) {
            throw new IllegalArgumentException(CatalogExceptionMessage.readOnlyAttribute(Entity.LOCATION, "name"));
        }
        if (updated.getService() == null || !original.getService().getId().equals(updated.getService().getId())) {
            throw new IllegalArgumentException(CatalogExceptionMessage.readOnlyAttribute(Entity.LOCATION, "service"));
        }
        // Validate new owner
        EntityReference newOwner = EntityUtil.populateOwner(userDAO(), teamDAO(), updated.getOwner());

        EntityReference newService = updated.getService();

        updated.setHref(null);
        updated.setOwner(null);
        updated.setService(null);
        // Remove previous tags.
        EntityUtil.removeTags(tagDAO(), original.getFullyQualifiedName());

        locationDAO().update(locationId, JsonUtils.pojoToJson(updated));
        updateOwner(updated, original.getOwner(), newOwner);
        updated.setService(newService);
        applyTags(updated);
    }

    public interface LocationDAO {
        @SqlUpdate("INSERT INTO location_entity (json) VALUES (:json)")
        void insert(@Bind("json") String json);

        @SqlUpdate("UPDATE location_entity SET  json = :json WHERE id = :id")
        void update(@Bind("id") String id, @Bind("json") String json);

        @SqlQuery("SELECT json FROM location_entity WHERE id = :locationId")
        String findById(@Bind("locationId") String locationId);

        @SqlQuery("SELECT json FROM location_entity WHERE fullyQualifiedName = :locationFQN")
        String findByFQN(@Bind("locationFQN") String locationFQN);

        @SqlQuery("SELECT count(*) FROM location_entity WHERE " +
                "(fullyQualifiedName LIKE CONCAT(:fqnPrefix, '%') OR :fqnPrefix IS NULL)") // Filter by prefix
        int listCount(@Bind("fqnPrefix") String fqnPrefix);

        @SqlQuery(
                "SELECT json FROM (" +
                        "SELECT fullyQualifiedName, json FROM location_entity WHERE " +
                        "(fullyQualifiedName LIKE CONCAT(:fqnPrefix, '%') OR :fqnPrefix IS NULL) AND " +// Filter by
                        // prefix
                        "fullyQualifiedName < :before " + // Pagination by location fullyQualifiedName
                        "ORDER BY fullyQualifiedName DESC " + // Pagination ordering by location fullyQualifiedName
                        "LIMIT :limit" +
                        ") last_rows_subquery ORDER BY fullyQualifiedName")
        List<String> listBefore(@Bind("fqnPrefix") String fqnPrefix, @Bind("limit") int limit,
                                @Bind("before") String before);

        @SqlQuery("SELECT json FROM location_entity WHERE " +
                "(fullyQualifiedName LIKE CONCAT(:fqnPrefix, '%') OR :fqnPrefix IS NULL) AND " +
                "fullyQualifiedName > :after " +
                "ORDER BY fullyQualifiedName " +
                "LIMIT :limit")
        List<String> listAfter(@Bind("fqnPrefix") String fqnPrefix, @Bind("limit") int limit,
                               @Bind("after") String after);

        @SqlQuery("SELECT EXISTS (SELECT * FROM location_entity WHERE id = :id)")
        boolean exists(@Bind("id") String id);

        @SqlUpdate("DELETE FROM location_entity WHERE id = :id")
        int delete(@Bind("id") String id);
    }
}
