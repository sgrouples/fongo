package com.github.fakemongo.test.beans;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Objects;

import java.util.List;

/**
 * @author rkolliva
 * @created 4/23/2016
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class TestParentBean extends AbstractTestBean {

    private String attr;

    private List<TestChildBean> secondaryCollItems;

    public String getAttr() {
        return attr;
    }

    public void setAttr(String attr) {
        this.attr = attr;
    }

    public List<TestChildBean> getSecondaryCollItems() {
        return secondaryCollItems;
    }

    public void setSecondaryCollItems(List<TestChildBean> secondaryCollItems) {
        this.secondaryCollItems = secondaryCollItems;
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
        TestParentBean that = (TestParentBean) o;
        return Objects.equal(attr, that.attr);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), attr);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InheritedAttributes {");
        final String parentStr = super.toString();
        sb.append(parentStr).append("}, ChildAttributes:{");
        sb.append("attr='").append(attr).append('\'');
        sb.append('}');
        return sb.toString();
    }

}
