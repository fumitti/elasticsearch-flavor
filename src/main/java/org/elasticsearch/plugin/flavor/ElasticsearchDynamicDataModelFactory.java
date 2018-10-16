package org.elasticsearch.plugin.flavor;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.rest.action.cat.RestPluginsAction;
import org.elasticsearch.rest.action.cat.RestTable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;

import org.elasticsearch.plugin.flavor.DataModelFactory;
import org.elasticsearch.search.sort.SortOrder;

public class ElasticsearchDynamicDataModelFactory implements DataModelFactory {
    private final FlavorRestAction action;
    private final RestRequest request;
    private final long startTime;
    private final RestChannel ch;
    private Logger logger = Loggers.getLogger(ElasticsearchDynamicDataModelFactory.class);
    private final Client client;

    private int scrollSize = 2000;
    private long keepAlive = 10000;

    public ElasticsearchDynamicDataModelFactory(final Client client, final RestChannel ch, final FlavorRestAction action, final RestRequest request, final long time) {
        this.client = client;
        this.ch = ch;
        this.action = action;
        this.request = request;
        this.startTime = time;
    }

    public void createItemBasedDataModel(final String index,
                                         final String type,
                                         final long itemId,
                                         final String operation) throws TasteException {
        client
                .prepareSearch(index)
                .setTypes(type)
                .addSort("_doc", SortOrder.ASC)
                .setScroll(new TimeValue(keepAlive))
                .setPostFilter(QueryBuilders.termQuery("item_id", itemId))
                .setFetchSource(new String[]{"user_id"}, null)
                .setSize(scrollSize)
                .execute(new RestActionListener<SearchResponse>(ch) {
                    public void processResponse(SearchResponse userIdsResponse) throws Exception {

                        final long numUsers = userIdsResponse.getHits().getTotalHits();
                        FastIDSet userIds = new FastIDSet((int) numUsers);
                        loop2(userIdsResponse, userIds);
                        client
                                .prepareSearch(index)
                                .setTypes(type)
                                .addSort("_doc", SortOrder.ASC)
                                .setPostFilter(QueryBuilders.termsQuery("user_id", userIds.toArray()))
                                .setFetchSource(new String[]{"user_id", "item_id", "value"}, null)
                                .setSize(scrollSize)
                                .setScroll(new TimeValue(keepAlive))
                                .execute(new RestActionListener<SearchResponse>(ch) {
                                    public void processResponse(SearchResponse scroll) throws Exception {

                                        final long total = scroll.getHits().getTotalHits();
                                        FastByIDMap<PreferenceArray> users = new FastByIDMap<PreferenceArray>((int) total);
                                        loop(scroll, users);
                                        switch (operation) {
                                            case "preload":
                                                action.renderStatus(channel, new GenericDataModel(users));
                                                break;
                                            case "similar_items":
                                                action.similar_items(new GenericDataModel(users), request, ch, startTime);
                                                break;
                                        }
                                    }
                                });

                    }
                });
    }

    public void createUserBasedDataModel(final String index,
                                         final String type,
                                         final long targetUserId,
                                         final String operation) throws TasteException {
        client
                .prepareSearch(index)
                .setTypes(type)
                .addSort("_doc", SortOrder.ASC)
                .setScroll(new TimeValue(keepAlive))
                .setPostFilter(QueryBuilders.termQuery("user_id", targetUserId))
                .setFetchSource(new String[]{"item_id"}, null)
                .setSize(scrollSize)
                .execute(new RestActionListener<SearchResponse>(ch) {
                    public void processResponse(SearchResponse itemIdsResponse) throws Exception {

                        final long numItems = itemIdsResponse.getHits().getTotalHits();
                        if (numItems <= 0) {
                            throw new NoSuchItemException("No such user_id:" + targetUserId);
                        }

                        FastIDSet itemIds = new FastIDSet((int) numItems);
                        while (true) {
                            for (SearchHit hit : itemIdsResponse.getHits().getHits()) {
                                final long itemId = getLongValue(hit, "item_id");
                                itemIds.add(itemId);
                            }
                            //Break condition: No hits are returned
                            itemIdsResponse = client
                                    .prepareSearchScroll(itemIdsResponse.getScrollId())
                                    .setScroll(new TimeValue(keepAlive))
                                    .execute()
                                    .actionGet();
                            if (itemIdsResponse.getHits().getHits().length == 0) {
                                break;
                            }
                        }

                        SearchResponse userIdsResponse = client
                                .prepareSearch(index)
                                .setTypes(type)
                                .addSort("_doc", SortOrder.ASC)
                                .setScroll(new TimeValue(keepAlive))
                                .setPostFilter(QueryBuilders.termsQuery("item_id", itemIds.toArray()))
                                .setFetchSource(new String[]{"user_id"}, null)
                                .setSize(scrollSize)
                                .execute()
                                .actionGet();
                        final long numUsers = userIdsResponse.getHits().getTotalHits();
                        FastIDSet userIds = new FastIDSet((int) numUsers);
                        loop2(userIdsResponse, userIds);
                        // logger.info("itemIds: {}, userIds: {}", itemIds, userIds);

                        client
                                .prepareSearch(index)
                                .setTypes(type)
                                .addSort("_doc", SortOrder.ASC)
                                .setPostFilter(QueryBuilders.termsQuery("user_id", userIds.toArray()))
                                .setFetchSource(new String[]{"user_id", "item_id", "value"}, null)
                                .setSize(scrollSize)
                                .setScroll(new TimeValue(keepAlive))
                                .execute(new RestActionListener<SearchResponse>(ch) {
                                    public void processResponse(SearchResponse scroll) throws Exception {

                                        final long total = scroll.getHits().getTotalHits();
                                        FastByIDMap<PreferenceArray> users = new FastByIDMap<PreferenceArray>((int) total);
                                        loop(scroll, users);
                                        switch (operation) {
                                            case "similar_users":
                                            case "user_based_recommend":
                                            case "item_based_recommend":
                                                action.similar_items(new GenericDataModel(users), request, ch, startTime);
                                                break;
                                        }
                                    }
                                });
                    }
                });
    }

    private void loop2(SearchResponse userIdsResponse, FastIDSet userIds) {
        while (true) {
            for (SearchHit hit : userIdsResponse.getHits().getHits()) {
                final long userId = getLongValue(hit, "user_id");
                userIds.add(userId);
            }
            //Break condition: No hits are returned
            userIdsResponse = client
                    .prepareSearchScroll(userIdsResponse.getScrollId())
                    .setScroll(new TimeValue(keepAlive))
                    .execute()
                    .actionGet();
            if (userIdsResponse.getHits().getHits().length == 0) {
                break;
            }
        }
    }

    private void loop(SearchResponse scroll, FastByIDMap<PreferenceArray> users) {
        while (true) {
            for (SearchHit hit : scroll.getHits().getHits()) {
                final long userId = getLongValue(hit, "user_id");
                final long itemId = getLongValue(hit, "item_id");
                final float value = getFloatValue(hit, "value");

                if (users.containsKey(userId)) {
                    GenericUserPreferenceArray user = (GenericUserPreferenceArray) users.get(userId);
                    GenericUserPreferenceArray newUser = new GenericUserPreferenceArray(user.length() + 1);
                    int currentLength = user.length();
                    for (int i = 0; i < currentLength; i++) {
                        newUser.setUserID(i, user.getUserID(i));
                        newUser.setItemID(i, user.getItemID(i));
                        newUser.setValue(i, user.getValue(i));
                    }
                    newUser.setUserID(currentLength, userId);
                    newUser.setItemID(currentLength, itemId);
                    newUser.setValue(currentLength, value);
                    users.put(userId, newUser);

                } else {
                    GenericUserPreferenceArray user = new GenericUserPreferenceArray(1);
                    user.setUserID(0, userId);
                    user.setItemID(0, itemId);
                    user.setValue(0, value);
                    users.put(userId, user);
                }
            }
            //Break condition: No hits are returned
            scroll = client
                    .prepareSearchScroll(scroll.getScrollId())
                    .setScroll(new TimeValue(keepAlive))
                    .execute()
                    .actionGet();
            if (scroll.getHits().getHits().length == 0) {
                break;
            }
        }
    }

    private long getLongValue(final SearchHit hit, final String field) {
        final DocumentField result = hit.field(field);
        if (result == null) {
            return 0;
        }
        final Number longValue = result.getValue();
        if (longValue == null) {
            return 0;
        }
        return longValue.longValue();
    }

    private float getFloatValue(final SearchHit hit, final String field) {
        final DocumentField result = hit.field(field);
        if (result == null) {
            return 0;
        }
        final Number floatValue = result.getValue();
        if (floatValue == null) {
            return 0;
        }
        return floatValue.floatValue();
    }
}
