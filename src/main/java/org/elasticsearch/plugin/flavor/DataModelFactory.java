package org.elasticsearch.plugin.flavor;

import org.apache.mahout.cf.taste.common.TasteException;

import org.apache.mahout.cf.taste.model.DataModel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;

public interface DataModelFactory {
    public void createItemBasedDataModel(final String index,
                                         final String type,
                                         final long itemId,
                                         final RestChannel ch,
                                         final long  startTime,
                                         final RestRequest request) throws TasteException;
    public void createUserBasedDataModel(final String index,
                                         final String type,
                                         final long userId,
                                         final RestChannel ch,
                                         final long  startTime,
                                         final RestRequest request) throws TasteException;
}
