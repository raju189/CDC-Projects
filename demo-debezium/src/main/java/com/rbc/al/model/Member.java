package com.rbc.al.model;

import java.io.Serializable;

public record Member(Long memberId, String name, String address, String email) implements Serializable {}
