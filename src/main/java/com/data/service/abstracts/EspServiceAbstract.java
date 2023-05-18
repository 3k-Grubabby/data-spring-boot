package com.data.service.abstracts;

import com.supconsoft.plantwrapex.ConnectionCreator;
import com.supconsoft.plantwrapex.IConnection;
import com.supconsoft.plantwrapex.PlantWrapException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public abstract class EspServiceAbstract extends DataServiceAbstract {
    @Override
    public IConnection getConnection(String url) {
        System.out.println("url = " + url);
        try {
            IConnection connection = ConnectionCreator.getConnection(url);
            return connection;
        } catch (PlantWrapException e) {
            e.printStackTrace();
        }
        return null;
    }
}
