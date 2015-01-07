/*
 * Copyright 2015 Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.app;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.onosproject.core.Permission;
import org.onosproject.core.Version;

import java.net.URI;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Basic tests of the default app description.
 */
public class DefaultApplicationDescriptionTest {

    public static final String APP_NAME = "org.foo.app";
    public static final Version VER = Version.version(1, 2, "a", null);
    public static final String DESC = "Awesome application from Circus";
    public static final String ORIGIN = "Circus";
    public static final Set<Permission> PERMS = ImmutableSet.of();
    public static final URI FURL = URI.create("mvn:org.foo-features/1.2a/xml/features");
    public static final Set<String> FEATURES = ImmutableSet.of("foo");

    @Test
    public void basics() {
        ApplicationDescription app =
                new DefaultApplicationDescription(APP_NAME, VER, DESC, ORIGIN,
                                                  PERMS, FURL, FEATURES);
        assertEquals("incorrect id", APP_NAME, app.name());
        assertEquals("incorrect version", VER, app.version());
        assertEquals("incorrect description", DESC, app.description());
        assertEquals("incorrect origin", ORIGIN, app.origin());
        assertEquals("incorrect permissions", PERMS, app.permissions());
        assertEquals("incorrect features repo", FURL, app.featuresRepo().get());
        assertEquals("incorrect features", FEATURES, app.features());
        assertTrue("incorrect toString", app.toString().contains(APP_NAME));
    }

}