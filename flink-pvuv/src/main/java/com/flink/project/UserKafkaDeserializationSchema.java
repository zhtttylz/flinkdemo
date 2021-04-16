package com.flink.project;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.io.Serializable;

public class UserKafkaDeserializationSchema<T> implements DeserializationSchema<UserBean>, SerializationSchema<UserBean> {

    private static final long serialVersionUID = 6154188370181669758L;

    @Override
    public UserBean deserialize(byte[] bytes) throws IOException {
        return JSON.parseObject(bytes, UserBean.class);
    }

    @Override
    public boolean isEndOfStream(UserBean userBean) {
        return false;
    }

    @Override
    public TypeInformation<UserBean> getProducedType() {
        return TypeInformation.of(UserBean.class);
    }

    @Override
    public byte[] serialize(UserBean userBean) {
        return userBean.toString().getBytes();
    }
}
