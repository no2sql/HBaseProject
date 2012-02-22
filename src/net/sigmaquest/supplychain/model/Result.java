//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vJAXB 2.1.10 in JDK 6 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2011.11.28 at 05:24:29 PM EST 
//


package net.sigmaquest.supplychain.model;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import com.thoughtworks.xstream.annotations.*;

/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;choice maxOccurs="unbounded" minOccurs="0">
 *           &lt;element ref="{urn:unitreport-schema}ValueDouble"/>
 *           &lt;element ref="{urn:unitreport-schema}ValueInteger"/>
 *           &lt;element ref="{urn:unitreport-schema}ValueString"/>
 *           &lt;element ref="{urn:unitreport-schema}ValueTimestamp"/>
 *           &lt;element ref="{urn:unitreport-schema}ValueBoolean"/>
 *           &lt;element ref="{urn:unitreport-schema}ValueRecord"/>
 *           &lt;element ref="{urn:unitreport-schema}ValueArray"/>
 *           &lt;element ref="{urn:unitreport-schema}ValueAttachment"/>
 *         &lt;/choice>
 *         &lt;element ref="{urn:unitreport-schema}Property" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "valueDoubleOrValueIntegerOrValueString",
    "property"
})
@XmlRootElement(name = "Result")
@XStreamAlias("Result")
public class Result {

    @XmlElements({
        @XmlElement(name = "ValueRecord", type = ValueRecord.class),
        @XmlElement(name = "ValueDouble", type = ValueDouble.class),
        @XmlElement(name = "ValueBoolean", type = ValueBoolean.class),
        @XmlElement(name = "ValueArray", type = ValueArray.class),
        @XmlElement(name = "ValueInteger", type = ValueInteger.class),
        @XmlElement(name = "ValueTimestamp", type = ValueTimestamp.class),
        @XmlElement(name = "ValueString", type = ValueString.class),
        @XmlElement(name = "ValueAttachment", type = ValueAttachment.class)
    })
    @XStreamImplicit
    protected List<Object> valueDoubleOrValueIntegerOrValueString;
    @XmlElement(name = "Property")
    protected Property property;

    /**
     * Gets the value of the valueDoubleOrValueIntegerOrValueString property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the valueDoubleOrValueIntegerOrValueString property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getValueDoubleOrValueIntegerOrValueString().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ValueRecord }
     * {@link ValueDouble }
     * {@link ValueBoolean }
     * {@link ValueArray }
     * {@link ValueInteger }
     * {@link ValueTimestamp }
     * {@link ValueString }
     * {@link ValueAttachment }
     * 
     * 
     */
    public List<Object> getValueDoubleOrValueIntegerOrValueString() {
        if (valueDoubleOrValueIntegerOrValueString == null) {
            valueDoubleOrValueIntegerOrValueString = new ArrayList<Object>();
        }
        return this.valueDoubleOrValueIntegerOrValueString;
    }

    /**
     * Gets the value of the property property.
     * 
     * @return
     *     possible object is
     *     {@link Property }
     *     
     */
    public Property getProperty() {
        return property;
    }

    /**
     * Sets the value of the property property.
     * 
     * @param value
     *     allowed object is
     *     {@link Property }
     *     
     */
    public void setProperty(Property value) {
        this.property = value;
    }

}
