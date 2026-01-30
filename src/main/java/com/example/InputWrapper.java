
package com.example;

import com.fasterxml.jackson.annotation.JsonProperty;

public class InputWrapper {
    @JsonProperty("Header")   // binds incoming JSON key "Header"
    private Header header;

    @JsonProperty("Body")     // binds incoming JSON key "Body"
    private Body body;

    public Header getHeader() { return header; }
    public void setHeader(Header header) { this.header = header; }

    public Body getBody() { return body; }
    public void setBody(Body body) { this.body = body; }
}
