package com.github.fakemongo.test.beans;

import com.google.common.base.Objects;

/**
 * @author Kollivakkam Raghavan
 * @created 4/23/2016
 */
public class TestChildBean extends TestParentBean {

    private String parentId;

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TestChildBean that = (TestChildBean) o;
        return Objects.equal(parentId, that.parentId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), parentId);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InheritedAttributes {");
        final String parentStr = super.toString();
        sb.append(parentStr).append("}, ChildAttributes:{");
        sb.append("parentId='").append(parentId).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
