/*
 * Copyright 2016-present Open Networking Laboratory
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

package org.onosproject.cli.net.vnet;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.onosproject.cli.AbstractShellCommand;
import org.onosproject.incubator.net.virtual.NetworkId;
import org.onosproject.incubator.net.virtual.VirtualNetworkAdminService;

/**
 * Removes a virtual network.
 */
@Command(scope = "onos", name = "vnet-remove",
        description = "Removes a virtual network.")
public class VirtualNetworkRemoveCommand extends AbstractShellCommand {

    @Argument(index = 0, name = "id", description = "Network ID",
            required = true, multiValued = false)
    Long id;

    @Override
    protected void execute() {
        VirtualNetworkAdminService service = get(VirtualNetworkAdminService.class);
        service.removeVirtualNetwork(NetworkId.networkId(id));
        print("Virtual network successfully removed.");
    }
}
