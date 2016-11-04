package com.sdu.stream.utils;

import java.util.Collection;

/**
 * Collection Util
 *
 * @author hanhan.zhang
 * */
public class CollectionUtil {

    public static final <E> boolean isEmpty(Collection<E> collection) {
        if (collection == null) {
            return true;
        }
        return collection.isEmpty();
    }

    public static final <E> boolean isNotEmpty(Collection<E> collection) {
        return !isEmpty(collection);
    }

}
