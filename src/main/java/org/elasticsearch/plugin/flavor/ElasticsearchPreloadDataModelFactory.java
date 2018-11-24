package org.elasticsearch.plugin.flavor;

import java.util.Map;
import java.security.InvalidParameterException;

import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.elasticsearch.client.Client;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.DataModel;

import com.google.gson.JsonObject;
import com.google.gson.JsonElement;

import org.elasticsearch.plugin.flavor.DataModelFactory;
import org.elasticsearch.plugin.flavor.ElasticsearchPreloadDataModel;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

public class ElasticsearchPreloadDataModelFactory implements DataModelFactory {
    private Logger logger = Loggers.getLogger(ElasticsearchPreloadDataModelFactory.class);
    private Client client;
    private String index = "preference";
    private String type = "preference";
    private DataModel dataModel;
    private final FlavorRestAction action;

    public ElasticsearchPreloadDataModelFactory(final Client client, final JsonObject settings, final FlavorRestAction action) {
        this.client = client;
        this.action = action;

        final JsonElement preferenceSettingsElement = settings.getAsJsonObject("preference");
        if (preferenceSettingsElement.isJsonNull()) {
            throw new InvalidParameterException("preference key not found.");
        }

        final JsonObject preferenceSettings = preferenceSettingsElement.getAsJsonObject();
        JsonElement preferenceIndexElement = preferenceSettings.get("index");
        if (preferenceIndexElement == null || preferenceIndexElement.isJsonNull()) {
            throw new InvalidParameterException("preference.index is null.");
        } else {
            this.index = preferenceIndexElement.getAsString();
        }

        JsonElement preferenceTypeElement = preferenceSettings.get("type");
        if (preferenceTypeElement != null && !preferenceTypeElement.isJsonNull()) {
            this.type = preferenceTypeElement.getAsString();
        }
        // if (settings.containsKey("keepAlive")) {
        //     dataModel.setKeepAlive(settings.get("keepAlive"));
        // }
        // if (settings.containsKey("scrollSize")) {
        //     dataModel.setScrollSize(settings.get("scrollSize"));
        // }

    }

    public void createItemBasedDataModel(final String _index,
                                         final String _type,
                                         final long _itemId,
                                         final RestChannel ch,
                                         final long startTime,
                                         final RestRequest request) throws TasteException {
        if (dataModel == null) {
            ElasticsearchPreloadDataModel preloadDataModel = new ElasticsearchPreloadDataModel(client, index, type);
            preloadDataModel.reload();
            this.dataModel = preloadDataModel;
        }

        final String operation = request.param("operation");
        if (operation ==null)
            action.renderStatus(ch, dataModel);
        else
        switch (operation) {
            case "preload":
                action.renderStatus(ch, dataModel);
                break;
            case "similar_items":
                action.similar_items(dataModel, request, ch, startTime);
                break;
            default:
                action.renderNotFound(ch, "Invalid operation: " + operation);
                break;
        }
    }

    public void createUserBasedDataModel(final String _index,
                                         final String _type,
                                         final long _userId,
                                         final RestChannel ch,
                                         final long startTime,
                                         final RestRequest request) throws TasteException {
        if (dataModel == null) {
            ElasticsearchPreloadDataModel preloadDataModel = new ElasticsearchPreloadDataModel(client, index, type);
            preloadDataModel.reload();
            this.dataModel = preloadDataModel;
        }
        final String operation = request.param("operation");
        switch (operation) {
            case "similar_users":
            case "user_based_recommend":
            case "item_based_recommend":
                action.similar_items(dataModel, request, ch, startTime);
                break;
            default:
                action.renderNotFound(ch, "Invalid operation: " + operation);
                break;
        }
    }
}
