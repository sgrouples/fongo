package com.github.fakemongo.test.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;

/**
 * @author Kollivakkam Raghavan
 * @created 4/23/2016
 */
public abstract class AbstractTestBean {

    @Id
    @JsonProperty("_id")
    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractTestBean that = (AbstractTestBean) o;
        return Objects.equal(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AbstractTestBean{");
        sb.append("id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
