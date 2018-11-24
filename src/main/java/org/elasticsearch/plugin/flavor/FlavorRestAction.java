package org.elasticsearch.plugin.flavor;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class FlavorRestAction extends BaseRestHandler {
    private DataModelFactory dataModelFactory;
    private Logger logger = Loggers.getLogger(FlavorRestAction.class);

    public FlavorRestAction(final Settings settings, final RestController controller) {
        super(settings);
        controller.registerHandler(POST, "/_flavor/preload", this);
        controller.registerHandler(GET,  "/{index}/{type}/_flavor/{operation}/{id}", this);
        controller.registerHandler(GET,  "/_flavor/{operation}/{id}", this);
    }


    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> {
            switch (request.method()) {
                case POST:
                    try {
                        final String jsonString = XContentHelper.convertToJson(request.content(), true);
                        JsonObject json = new Gson().fromJson(jsonString, JsonObject.class);
                        final long startTime = System.currentTimeMillis();

                        ElasticsearchPreloadDataModelFactory factory = new ElasticsearchPreloadDataModelFactory(client, json,this);
                        this.dataModelFactory = factory;
                        factory.createItemBasedDataModel(null, null, 0,channel,startTime,request);

                    } catch (final Exception e) {
                        handleErrorRequest(channel, e);
                    }
                    break;
                case GET:
                    try {
                        final String operation = request.param("operation");
                        final String index = request.param("index");
                        final String type = request.param("type");
                        final long id = request.paramAsLong("id", 0);
                        final int size = request.paramAsInt("size", 10);

                        final long startTime = System.currentTimeMillis();

                        final RecommenderBuilder builder = RecommenderBuilder
                                .builder()
                                .similarity(request.param("similarity"))
                                .neighborhood(request.param("neighborhood"))
                                .neighborhoodNearestN(request.paramAsInt("neighborhoodN", 10))
                                .neighborhoodThreshold((double) request.paramAsFloat("neighborhoodThreshold", 0.1F));

                        if(dataModelFactory == null){
                            ElasticsearchDynamicDataModelFactory factory = new ElasticsearchDynamicDataModelFactory(client,this);
                            this.dataModelFactory = factory;
                        }

                        switch (operation) {
                            case "similar_items":
                                dataModelFactory.createItemBasedDataModel(index, type, id, channel, startTime, request);
                                break;
                            case "similar_users":
                            case "user_based_recommend":
                            case "item_based_recommend":
                                dataModelFactory.createUserBasedDataModel(index, type, id, channel, startTime, request);
                                break;
                            default:
                                renderNotFound(channel, "Invalid operation: " + operation);
                                break;
                        }

                    } catch (final NoSuchItemException e) {
                        renderNotFound(channel, e.toString());
                    } catch (final Exception e) {
                        handleErrorRequest(channel, e);
                    }
                    break;
                default:
                    renderNotFound(channel, "No such action");
                    break;
            }
        };
    }

    private void renderRecommendedItems(final RestChannel channel,
                                        final List<RecommendedItem> items,
                                        final long startTime) {
        try {
            final XContentBuilder builder = JsonXContent.contentBuilder();
            builder
                .startObject()
                .field("took", System.currentTimeMillis() - startTime)
                .startObject("hits")
                .field("total", items.size())
                .startArray("hits");
            for (final RecommendedItem item : items) {
                builder
                    .startObject()
                    .field("item_id", item.getItemID())
                    .field("value", item.getValue())
                    .endObject();
            }
            builder
                .endArray()
                .endObject()
                .endObject();
            channel.sendResponse(new BytesRestResponse(OK, builder));

        } catch(final Exception e) {
            handleErrorRequest(channel, e);
        }
    }

    private void renderUserIds(final RestChannel channel,
                               final long[] userIds,
                               final long startTime) {
        try {
            final XContentBuilder builder = JsonXContent.contentBuilder();
            builder
                .startObject()
                .field("took", System.currentTimeMillis() - startTime)
                .startObject("hits")
                .field("total", userIds.length)
                .startArray("hits");
            for (int i = 0; i < userIds.length; i++) {
                builder
                    .startObject()
                    .field("user_id", userIds[i])
                    .endObject();
            }
            builder
                .endArray()
                .endObject();
            channel.sendResponse(new BytesRestResponse(OK, builder));

        } catch(final Exception e) {
            handleErrorRequest(channel, e);
        }
    }

    protected void renderNotFound(final RestChannel channel, final String message) {
        try {
            // 404
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder
                .startObject()
                .field("error", message)
                .field("status", 404)
                .endObject();
            channel.sendResponse(new BytesRestResponse(NOT_FOUND, builder));
        } catch (final IOException e) {
            handleErrorRequest(channel, e);
        }
    }

    protected void renderStatus(final RestChannel channel, final DataModel dataModel) {
        try {
            final XContentBuilder builder = JsonXContent.contentBuilder();
            builder
                .startObject()
                .field("preloadDataModel", dataModel.toString())
                .field("total_users", dataModel.getNumUsers())
                .field("total_items", dataModel.getNumItems())
                .endObject();
            channel.sendResponse(new BytesRestResponse(OK, builder));
        } catch (final Exception e) {
            handleErrorRequest(channel, e);
        }
    }

    private void handleErrorRequest(final RestChannel channel, final Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (final IOException e1) {
            logger.error("Failed to send a failure response.", e1);
        }
    }

    @Override
    public String getName() {
        return "flavor";
    }

    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>();
        responseParams.add("operation");
        responseParams.add("index");
        responseParams.add("type");
        responseParams.add("id");
        responseParams.add("size");
        responseParams.add("similarity");
        responseParams.add("neighborhood");
        responseParams.add("neighborhoodN");
        responseParams.add("neighborhoodThreshold");
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    protected void similar_items(DataModel dataModelFromUserIds, RestRequest request, RestChannel channel, long startTime) throws TasteException {

        final String operation = request.param("operation");
        final String index = request.param("index");
        final String type = request.param("type");
        final long id = request.paramAsLong("id", 0);
        final int size = request.paramAsInt("size", 10);

        final RecommenderBuilder builder = RecommenderBuilder
                .builder()
                .similarity(request.param("similarity"))
                .neighborhood(request.param("neighborhood"))
                .neighborhoodNearestN(request.paramAsInt("neighborhoodN", 10))
                .neighborhoodThreshold((double) request.paramAsFloat("neighborhoodThreshold", 0.1F));


        if (operation.equals("similar_items")) {

            ItemBasedRecommender recommender = builder
                    .dataModel(dataModelFromUserIds)
                    .itemBasedRecommender();

            List<RecommendedItem> items = recommender.mostSimilarItems(id, size);
            renderRecommendedItems(channel, items, startTime);

        } else if (operation.equals("similar_users")) {
            UserBasedRecommender recommender = builder
                    .dataModel(dataModelFromUserIds)
                    .userBasedRecommender();

            long[] userIds = recommender.mostSimilarUserIDs(id, size);
            renderUserIds(channel, userIds, startTime);

        } else if (operation.equals("user_based_recommend")) {
            UserBasedRecommender recommender = builder
                    .dataModel(dataModelFromUserIds)
                    .userBasedRecommender();

            List<RecommendedItem> items = recommender.recommend(id, size);
            renderRecommendedItems(channel, items, startTime);


        } else if (operation.equals("item_based_recommend")) {
            ItemBasedRecommender recommender = builder
                    .dataModel(dataModelFromUserIds)
                    .itemBasedRecommender();

            List<RecommendedItem> items = recommender.recommend(id, size);
            renderRecommendedItems(channel, items, startTime);

        } else {
            renderNotFound(channel, "Invalid operation: " + operation);
        }
    }
}
