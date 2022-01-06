/*
 *  Copyright 2021 Collate
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.openmetadata.catalog.resources.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.openmetadata.catalog.util.TestUtils.adminAuthHeaders;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.ws.rs.core.Response;
import org.apache.http.client.HttpResponseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.openmetadata.catalog.Entity;
import org.openmetadata.catalog.api.events.CreateWebhook;
import org.openmetadata.catalog.jdbi3.WebhookRepository.WebhookEntityInterface;
import org.openmetadata.catalog.resources.EntityResourceTest;
import org.openmetadata.catalog.resources.events.WebhookCallbackResource.EventDetails;
import org.openmetadata.catalog.resources.events.WebhookResource.WebhookList;
import org.openmetadata.catalog.type.ChangeDescription;
import org.openmetadata.catalog.type.ChangeEvent;
import org.openmetadata.catalog.type.EntityReference;
import org.openmetadata.catalog.type.EventFilter;
import org.openmetadata.catalog.type.EventType;
import org.openmetadata.catalog.type.FailureDetails;
import org.openmetadata.catalog.type.FieldChange;
import org.openmetadata.catalog.type.Webhook;
import org.openmetadata.catalog.type.Webhook.Status;
import org.openmetadata.catalog.util.EntityInterface;
import org.openmetadata.catalog.util.JsonUtils;
import org.openmetadata.catalog.util.TestUtils;
import org.openmetadata.catalog.util.TestUtils.UpdateType;

public class WebhookResourceTest extends EntityResourceTest<Webhook> {
  public static List<EventFilter> ALL_EVENTS_FILTER = new ArrayList<>();

  static {
    ALL_EVENTS_FILTER.add(new EventFilter().withEventType(EventType.ENTITY_CREATED).withEntities(List.of("*")));
    ALL_EVENTS_FILTER.add(new EventFilter().withEventType(EventType.ENTITY_UPDATED).withEntities(List.of("*")));
    ALL_EVENTS_FILTER.add(new EventFilter().withEventType(EventType.ENTITY_DELETED).withEntities(List.of("*")));
  }

  public WebhookResourceTest() {
    super(Entity.WEBHOOK, Webhook.class, WebhookList.class, "webhook", "", false, false, false);
    supportsPatch = false;
  }

  @Test
  void post_webhookEnabledStateChange(TestInfo test) throws URISyntaxException, IOException, InterruptedException {
    //
    // Create webhook in disabled state. It will not start webhook publisher
    //
    String webhookName = getEntityName(test);
    LOG.info("creating webhook in disabled state");
    String uri = "http://localhost:" + APP.getLocalPort() + "/api/v1/test/webhook/" + webhookName;
    CreateWebhook create = createRequest(webhookName, "", "", null).withEnabled(false).withEndpoint(URI.create(uri));
    Webhook webhook = createAndCheckEntity(create, adminAuthHeaders());
    assertEquals(Status.NOT_STARTED, webhook.getStatus());
    Webhook getWebhook = getEntity(webhook.getId(), adminAuthHeaders());
    assertEquals(Status.NOT_STARTED, getWebhook.getStatus());
    EventDetails details = webhookCallbackResource.getEventDetails(webhookName);
    assertNull(details);

    //
    // Now enable the webhook
    //
    LOG.info("Enabling webhook");
    ChangeDescription change = getChangeDescription(webhook.getVersion());
    change.getFieldsUpdated().add(new FieldChange().withName("enabled").withOldValue(false).withNewValue(true));
    change
        .getFieldsUpdated()
        .add(new FieldChange().withName("status").withOldValue(Status.NOT_STARTED).withNewValue(Status.STARTED));
    change.getFieldsUpdated().add(new FieldChange().withName("batchSize").withOldValue(10).withNewValue(50));
    create.withEnabled(true).withBatchSize(50);

    webhook = updateAndCheckEntity(create, Response.Status.OK, adminAuthHeaders(), UpdateType.MINOR_UPDATE, change);
    assertEquals(Status.STARTED, webhook.getStatus());
    getWebhook = getEntity(webhook.getId(), adminAuthHeaders());
    assertEquals(Status.STARTED, getWebhook.getStatus());

    // Ensure the call back notification has started
    details = waitForFirstEvent(webhookName, 25, 100);
    assertEquals(1, details.getEvents().size());
    long lastSuccessfulEventTime = details.getLatestEventTime();
    FailureDetails failureDetails = new FailureDetails().withLastSuccessfulAt(lastSuccessfulEventTime);

    //
    // Disable the webhook and ensure notification is disabled
    //
    LOG.info("Disabling webhook");
    create.withEnabled(false);
    change = getChangeDescription(getWebhook.getVersion());
    change
        .getFieldsAdded()
        .add(new FieldChange().withName("failureDetails").withNewValue(JsonUtils.pojoToJson(failureDetails)));
    change.getFieldsUpdated().add(new FieldChange().withName("enabled").withOldValue(true).withNewValue(false));
    change
        .getFieldsUpdated()
        .add(new FieldChange().withName("status").withOldValue(Status.STARTED).withNewValue(Status.NOT_STARTED));

    // Disabled webhook state is NOT_STARTED
    getWebhook = updateAndCheckEntity(create, Response.Status.OK, adminAuthHeaders(), UpdateType.MINOR_UPDATE, change);
    assertEquals(Status.NOT_STARTED, getWebhook.getStatus());

    // Disabled webhook state also records last successful time when event was sent
    getWebhook = getEntity(webhook.getId(), adminAuthHeaders());
    assertEquals(Status.NOT_STARTED, getWebhook.getStatus());
    assertEquals(details.getFirstEventTime(), getWebhook.getFailureDetails().getLastSuccessfulAt());

    // Ensure callback back notification is disabled with no new events
    int iterations = 0;
    while (iterations < 100) {
      Thread.sleep(10);
      iterations++;
      assertEquals(1, details.getEvents().size()); // Event counter remains the same
    }

    deleteEntity(webhook.getId(), adminAuthHeaders());
  }

  @Test
  void put_updateEndpointURL(TestInfo test) throws URISyntaxException, IOException, InterruptedException {
    CreateWebhook create =
        createRequest("counter", "", "", null).withEnabled(true).withEndpoint(URI.create("http://invalidUnknowHost"));
    Webhook webhook = createAndCheckEntity(create, adminAuthHeaders());

    // Wait for webhook to be marked as failed
    int iteration = 0;
    Webhook getWebhook = getEntity(webhook.getId(), adminAuthHeaders());
    LOG.info("getWebhook {}", getWebhook);
    while (getWebhook.getStatus() != Status.FAILED && iteration < 100) {
      getWebhook = getEntity(webhook.getId(), adminAuthHeaders());
      LOG.info("getWebhook {}", getWebhook);
      Thread.sleep(100);
      iteration++;
    }
    assertEquals(Status.FAILED, getWebhook.getStatus());

    // Now change the webhook URL to a valid URL and ensure callbacks resume
    String baseUri = "http://localhost:" + APP.getLocalPort() + "/api/v1/test/webhook/counter/" + test.getDisplayName();
    create = create.withEndpoint(URI.create(baseUri));
    ChangeDescription change = getChangeDescription(getWebhook.getVersion());
    change
        .getFieldsUpdated()
        .add(
            new FieldChange()
                .withName("endPoint")
                .withOldValue(webhook.getEndpoint())
                .withNewValue(create.getEndpoint()));
    change
        .getFieldsUpdated()
        .add(new FieldChange().withName("status").withOldValue(Status.FAILED).withNewValue(Status.STARTED));
    webhook = updateAndCheckEntity(create, Response.Status.OK, adminAuthHeaders(), UpdateType.MINOR_UPDATE, change);

    deleteEntity(webhook.getId(), adminAuthHeaders());
  }

  @Override
  public CreateWebhook createRequest(String name, String description, String displayName, EntityReference owner)
      throws URISyntaxException {
    String uri = "http://localhost:" + APP.getLocalPort() + "/api/v1/test/webhook/ignore";
    return new CreateWebhook()
        .withName(name)
        .withDescription(description)
        .withEventFilters(ALL_EVENTS_FILTER)
        .withEndpoint(URI.create(uri))
        .withBatchSize(100)
        .withEnabled(false);
  }

  @Override
  public EntityReference getContainer(Object createRequest) throws URISyntaxException {
    return null; // No container entity
  }

  @Override
  public void validateCreatedEntity(Webhook webhook, Object request, Map<String, String> authHeaders)
      throws HttpResponseException {
    CreateWebhook createRequest = (CreateWebhook) request;
    validateCommonEntityFields(
        getEntityInterface(webhook), createRequest.getDescription(), TestUtils.getPrincipal(authHeaders), null);
    assertEquals(createRequest.getName(), webhook.getName());
    assertEquals(createRequest.getEventFilters(), webhook.getEventFilters());
  }

  @Override
  public void validateUpdatedEntity(Webhook webhook, Object request, Map<String, String> authHeaders)
      throws HttpResponseException {
    validateCreatedEntity(webhook, request, authHeaders);
  }

  @Override
  public void compareEntities(Webhook expected, Webhook updated, Map<String, String> authHeaders)
      throws HttpResponseException {
    // Patch not supported
  }

  @Override
  public EntityInterface<Webhook> getEntityInterface(Webhook entity) {
    return new WebhookEntityInterface(entity);
  }

  @Override
  public void validateGetWithDifferentFields(Webhook entity, boolean byName) throws HttpResponseException {}

  @Override
  public void assertFieldChange(String fieldName, Object expected, Object actual) throws IOException {}

  /**
   * Before a test for every entity resource, create a webhook subscription. At the end of the test, ensure all events
   * are delivered over web subscription comparing it with number of events stored in the system.
   */
  public void startWebhookSubscription() throws IOException, URISyntaxException {
    // Valid webhook callback
    String baseUri = "http://localhost:" + APP.getLocalPort() + "/api/v1/test/webhook/healthy";
    createWebhook("healthy", baseUri);
  }

  /** Start webhook subscription for given entity and various event types */
  public void startWebhookEntitySubscriptions(String entity) throws IOException, URISyntaxException {
    String baseUri = "http://localhost:" + APP.getLocalPort() + "/api/v1/test/webhook/filterBased";

    // Create webhook with endpoint api/v1/test/webhook/entityCreated/<entity> to receive entityCreated events
    String name = EventType.ENTITY_CREATED + ":" + entity;
    String uri = baseUri + "/" + EventType.ENTITY_CREATED + "/" + entity;
    List<EventFilter> filters =
        List.of(new EventFilter().withEventType(EventType.ENTITY_CREATED).withEntities(List.of(entity)));
    createWebhook(name, uri, filters);

    // Create webhook with endpoint api/v1/test/webhook/entityUpdated/<entity> to receive entityUpdated events
    name = EventType.ENTITY_UPDATED + ":" + entity;
    uri = baseUri + "/" + EventType.ENTITY_UPDATED + "/" + entity;
    filters = List.of(new EventFilter().withEventType(EventType.ENTITY_UPDATED).withEntities(List.of(entity)));
    createWebhook(name, uri, filters);

    // TODO entity deleted events
  }

  /**
   * At the end of the test, ensure all events are delivered over web subscription comparing it with number of events
   * stored in the system.
   */
  public void validateWebhookEvents() throws HttpResponseException, InterruptedException {
    // Check the healthy callback server received all the change events
    EventDetails details = webhookCallbackResource.getEventDetails("healthy");
    assertNotNull(details);
    ConcurrentLinkedQueue<ChangeEvent> callbackEvents = details.getEvents();
    assertNotNull(callbackEvents);
    assertNotNull(callbackEvents.peek());
    List<ChangeEvent> actualEvents =
        getChangeEvents("*", "*", "*", callbackEvents.peek().getDateTime(), adminAuthHeaders()).getData();
    waitAndCheckForEvents(callbackEvents, actualEvents, 10, 100);
    assertWebhookStatusSuccess("healthy");
  }

  /** At the end of the test, ensure all events are delivered for the combination of entity and eventTypes */
  public void validateWebhookEntityEvents(String entity) throws HttpResponseException, InterruptedException {
    // Check the healthy callback server received all the change events
    // For the entity all the webhooks registered for created events have the right number of events
    List<ChangeEvent> callbackEvents =
        webhookCallbackResource.getEntityCallbackEvents(EventType.ENTITY_CREATED, entity);
    Date date = callbackEvents.get(0).getDateTime();
    List<ChangeEvent> events = getChangeEvents(entity, null, null, date, adminAuthHeaders()).getData();
    waitAndCheckForEvents(callbackEvents, events, 30, 100);

    // For the entity all the webhooks registered for updated events have the right number of events
    callbackEvents = webhookCallbackResource.getEntityCallbackEvents(EventType.ENTITY_UPDATED, entity);
    // Use previous date if no update events
    date = callbackEvents.size() > 0 ? callbackEvents.get(0).getDateTime() : date;
    events = getChangeEvents(null, entity, null, date, adminAuthHeaders()).getData();
    waitAndCheckForEvents(callbackEvents, events, 30, 100);

    // TODO add delete event support
  }

  @Test
  void testDifferentTypesOfWebhooks() throws IOException, InterruptedException, URISyntaxException {
    String baseUri = "http://localhost:" + APP.getLocalPort() + "/api/v1/test/webhook";

    // Create multiple webhooks each with different type of response to callback
    Webhook w1 = createWebhook("slowServer", baseUri + "/simulate/slowServer"); // Callback response 1 second slower
    Webhook w2 = createWebhook("callbackTimeout", baseUri + "/simulate/timeout"); // Callback response 12 seconds slower
    Webhook w3 = createWebhook("callbackResponse300", baseUri + "/simulate/300"); // 3xx response
    Webhook w4 = createWebhook("callbackResponse400", baseUri + "/simulate/400"); // 4xx response
    Webhook w5 = createWebhook("callbackResponse500", baseUri + "/simulate/500"); // 5xx response
    Webhook w6 = createWebhook("invalidEndpoint", "http://invalidUnknownHost"); // Invalid URL

    Thread.sleep(1000);

    // Now check state of webhooks created
    EventDetails details = waitForFirstEvent("simulate-slowServer", 25, 100);
    ConcurrentLinkedQueue<ChangeEvent> callbackEvents = details.getEvents();
    assertNotNull(callbackEvents.peek());

    List<ChangeEvent> actualEvents =
        getChangeEvents("*", "*", "*", callbackEvents.peek().getDateTime(), adminAuthHeaders()).getData();
    waitAndCheckForEvents(callbackEvents, actualEvents, 30, 100);

    // Check all webhook status
    assertWebhookStatusSuccess("slowServer");
    assertWebhookStatus("callbackResponse300", Status.FAILED, 301, "Moved Permanently");
    assertWebhookStatus("callbackResponse400", Status.AWAITING_RETRY, 400, "Bad Request");
    assertWebhookStatus("callbackResponse500", Status.AWAITING_RETRY, 500, "Internal Server Error");
    assertWebhookStatus("invalidEndpoint", Status.FAILED, null, "UnknownHostException");

    // Delete all webhooks
    deleteEntity(w1.getId(), adminAuthHeaders());
    deleteEntity(w2.getId(), adminAuthHeaders());
    deleteEntity(w3.getId(), adminAuthHeaders());
    deleteEntity(w4.getId(), adminAuthHeaders());
    deleteEntity(w5.getId(), adminAuthHeaders());
    deleteEntity(w6.getId(), adminAuthHeaders());
  }

  public Webhook createWebhook(String name, String uri) throws URISyntaxException, IOException {
    return createWebhook(name, uri, ALL_EVENTS_FILTER);
  }

  public Webhook createWebhook(String name, String uri, List<EventFilter> filters)
      throws URISyntaxException, IOException {
    CreateWebhook createWebhook =
        createRequest(name, "", "", null).withEndpoint(URI.create(uri)).withEventFilters(filters).withEnabled(true);
    return createAndCheckEntity(createWebhook, adminAuthHeaders());
  }

  public void assertWebhookStatusSuccess(String name) throws HttpResponseException {
    Webhook webhook = getEntityByName(name, "", adminAuthHeaders());
    assertEquals(Status.STARTED, webhook.getStatus());
    assertNull(webhook.getFailureDetails());
  }

  public void assertWebhookStatus(String name, Status status, Integer statusCode, String failedReason)
      throws HttpResponseException {
    Webhook webhook = getEntityByName(name, "", adminAuthHeaders());
    assertEquals(status, webhook.getStatus());
    assertEquals(statusCode, webhook.getFailureDetails().getLastFailedStatusCode());
    assertEquals(failedReason, webhook.getFailureDetails().getLastFailedReason());
  }

  public void waitAndCheckForEvents(
      Collection<ChangeEvent> expected, Collection<ChangeEvent> received, int iteration, long sleepMillis)
      throws InterruptedException {
    int i = 0;
    while (expected.size() < received.size() && i < iteration) {
      Thread.sleep(sleepMillis);
      i++;
    }
    if (expected.size() != received.size()) {
      expected.forEach(
          c1 ->
              LOG.info(
                  "expected {}:{}:{}:{}",
                  c1.getDateTime().getTime(),
                  c1.getEventType(),
                  c1.getEntityType(),
                  c1.getEntityId()));
      received.forEach(
          c1 ->
              LOG.info(
                  "received {}:{}:{}:{}",
                  c1.getDateTime().getTime(),
                  c1.getEventType(),
                  c1.getEntityType(),
                  c1.getEntityId()));
    }
    assertEquals(expected.size(), received.size());
  }

  public EventDetails waitForFirstEvent(String endpoint, int iteration, long sleepMillis) throws InterruptedException {
    EventDetails details = webhookCallbackResource.getEventDetails(endpoint);
    int i = 0;
    while ((details == null || details.getEvents() == null || details.getEvents().size() <= 0) && i < iteration) {
      details = webhookCallbackResource.getEventDetails(endpoint);
      Thread.sleep(sleepMillis);
      i++;
    }
    LOG.info("Returning for endpoint {} eventDetails {}", endpoint, details);
    return details;
  }
}
