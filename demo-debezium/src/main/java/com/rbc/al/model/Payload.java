package com.rbc.al.model;


import java.io.Serializable;

public  record Payload(Member before, Member after, String operation) implements Serializable {

}


