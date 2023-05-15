package com.data.manager.impl;

import com.data.manager.Connection;
import com.supconsoft.plantwrapex.ConnectionCreator;
import com.supconsoft.plantwrapex.IConnection;
import com.supconsoft.plantwrapex.PlantWrapException;
import org.springframework.stereotype.Component;

@Component
public class EspConnectionImpl implements Connection<IConnection> {

    private IConnection connection = null;

    @Override
    public void connect(String url) {
        try {
            connection = ConnectionCreator.getConnection(url);
        } catch (PlantWrapException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        connection.close();
    }

    @Override
    public IConnection getConnection() {
        return connection;
    }
}
