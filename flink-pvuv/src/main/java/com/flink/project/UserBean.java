package com.flink.project;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class UserBean {

    private String action;
    private String categroy;
    private int itemId;
    private long ts;
    private int userId;
}
