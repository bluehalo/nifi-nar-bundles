/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asymmetrik.nifi.processors;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.openqa.selenium.By;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@SideEffectFree
@Tags({"get", "http", "xml"})
@CapabilityDescription("Uses PhantomJS to make a webpage request and sends the page source to the success relationship. It " +
        "should be noted that this processor is capable of getting the contents of an AngularJS webpage, or" +
        "other single-page application.")
public class GetWebpage extends AbstractProcessor {
    private static final String BY_XPATH = "xpath";
    private static final String BY_CSS = "css";
    private static final String BY_ID = "id";
    private static final String BY_LINK_TEXT = "link text";

    /**
     * Relationships
     */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The page source of the website.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("failures")
            .build();

    /**
     * Property Descriptors
     */
    private static final PropertyDescriptor WEBSITE_URL = new PropertyDescriptor.Builder()
            .name("Webpage URL")
            .description("The URL of the website to scrape")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private static final PropertyDescriptor DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("PhantonJS Driver Location")
            .description("Location of the phantonjs driver executable.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    private static final PropertyDescriptor SELECTOR_TYPE = new PropertyDescriptor.Builder()
            .name("Selector Type")
            .description("The type of selector to use to test for visibility. ")
            .required(true)
            .allowableValues(BY_XPATH, BY_CSS, BY_ID, BY_LINK_TEXT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(BY_XPATH)
            .build();

    private static final PropertyDescriptor SELECTOR = new PropertyDescriptor.Builder()
            .name("Selector")
            .description("The selector, of type specified by the selector type property, to use to test for visibility. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("/html/body/div")
            .build();

    private static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Timeout")
            .description("The length of time to wait for a response")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 sec")
            .build();

    private String url;
    private Long timeout;
    private String selector;
    private String selectorType;
    private PhantomJSDriver driver;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        url = context.getProperty(WEBSITE_URL).getValue();

        timeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.SECONDS);
        selectorType = context.getProperty(SELECTOR_TYPE).getValue();
        selector = context.getProperty(SELECTOR).getValue();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) {
        DesiredCapabilities DesireCaps = new DesiredCapabilities();
        DesireCaps.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, context.getProperty(DRIVER_LOCATION).getValue());
        driver = new PhantomJSDriver(DesireCaps);
        FlowFile flowFile = session.create();
        try {
            driver.get(url);
            (new WebDriverWait(driver, timeout)).until(
                    ExpectedConditions.visibilityOfElementLocated(getExpectedCondition(selectorType, selector))
            );

            final byte[] page = formatToXHtml(driver.getPageSource(), StandardCharsets.UTF_8);
            flowFile = session.write(flowFile, outputStream -> outputStream.write(page));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            flowFile = session.write(flowFile, outputStream -> outputStream.write(e.getMessage().getBytes()));
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            driver.quit();
        }
        session.getProvenanceReporter().create(flowFile);
    }

    @OnStopped
    public void onStopped() {
        driver.quit();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(WEBSITE_URL, DRIVER_LOCATION, SELECTOR_TYPE, SELECTOR, TIMEOUT));
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>(2);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return Collections.unmodifiableSet(relationships);
    }

    private By getExpectedCondition(final String selectorType, final String selector) {
        switch (selectorType) {
            case BY_XPATH:
                return By.xpath(selector);
            case BY_CSS:
                return By.cssSelector(selector);
            case BY_ID:
                return By.id(selector);
            case BY_LINK_TEXT:
                return By.linkText(selector);
        }
        return By.xpath(selector);
    }

    /**
     * Uses Jsoup to convert from HTML to XHTML
     */
    private byte[] formatToXHtml(String html, Charset charset) {
        Document document = Jsoup.parseBodyFragment(html);
        document.outputSettings().syntax(Document.OutputSettings.Syntax.xml);
        document.outputSettings().charset(charset);
        return document.toString().getBytes(charset);
    }

}
