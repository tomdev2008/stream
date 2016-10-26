package com.sdu.stream.trident.custom.state;

import org.apache.storm.trident.state.State;

import java.util.List;

/**
 * Location State
 * */
public class LocationDb implements State {

    // next location state transaction id
    private long _bTxId;

    // current location state update transaction id
    private long _cTxId;

    @Override
    public void beginCommit(Long txid) {
        this._bTxId = txid;
    }

    @Override
    public void commit(Long txid) {
        this._cTxId = txid;
    }

    public void setBulkLocation(List<Long> userIds, List<String> locations) {
        // code to access database and set location
        StringBuffer sb = new StringBuffer();
        sb.append("bTxId=").append(_bTxId).append("\t");
        sb.append("cTxId=").append(_cTxId).append("\t");
        sb.append("userLocation=[");
        boolean first = true;
        for (int i = 0; i < userIds.size(); i++) {
            if (first) {
                sb.append(userIds.get(i)).append(":").append(locations.get(i));
                first = false;
            } else {
                sb.append(",").append(userIds.get(i)).append(":").append(locations.get(i));
            }
        }
        sb.append("]");
        System.out.println(sb.toString());
    }

    public List<String> getBulkLocation(List<Long> userIds) {
        // code to get location from database
        return null;
    }
}
