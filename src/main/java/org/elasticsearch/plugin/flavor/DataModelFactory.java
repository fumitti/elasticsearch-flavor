package org.elasticsearch.plugin.flavor;

import org.apache.mahout.cf.taste.common.TasteException;

import org.apache.mahout.cf.taste.model.DataModel;

public interface DataModelFactory {
    public void createItemBasedDataModel(final String index,
                                              final String type,
                                              final long itemId,
                                              final String operation) throws TasteException;
    public void createUserBasedDataModel(final String index,
                                              final String type,
                                              final long userId,
                                              final String operation) throws TasteException;
}
