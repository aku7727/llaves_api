
package com.example;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Header {
    @JsonProperty("XRqUID")   // enforce exact key casing from JSON
    private String XRqUID;

    public String getXRqUID() { return XRqUID; }
    public void setXRqUID(String XRqUID) { this.XRqUID = XRqUID; }
}
