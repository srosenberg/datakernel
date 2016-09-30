/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.util;

import io.datakernel.http.HttpHeaders;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpUtils;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;

public class GetRealIpTest {

	@Test
	public void handlesXForwardedForHeader() {
		String xForwardedForAddr = "10.15.20.25";
		HttpRequest request =
				HttpRequest.get("http://85.75.65.55/id").withHeader(HttpHeaders.X_FORWARDED_FOR, xForwardedForAddr);

		InetAddress realIP = HttpUtils.getRealIp(request);

		assertEquals(realIP, HttpUtils.inetAddress(xForwardedForAddr));
	}

	@Test
	public void handlesForwardedHeaderWithIPv4() {
		String forwardedOriginalAddr = "10.15.20.25";
		String forwardedValue = "   for=" + forwardedOriginalAddr + ";proto=http, for=150.78.5.3;by=7.8.5.1";

		HttpRequest request =
				HttpRequest.get("http://85.75.65.55/id").withHeader(HttpHeaders.FORWARDED, forwardedValue);

		InetAddress realIP = HttpUtils.getRealIp(request);
		assertEquals(realIP, HttpUtils.inetAddress(forwardedOriginalAddr));
	}

	@Test
	public void handlesForwardedHeaderWithIPv6() {
		String forwardedOriginalAddrIPv6 = "aa::bb";
		String forwardedValue = "   for=\"[" + forwardedOriginalAddrIPv6 + "]\";proto=http, for=150.78.5.3;by=7.8.5.1";

		HttpRequest request =
				HttpRequest.get("http://85.75.65.55/id").withHeader(HttpHeaders.FORWARDED, forwardedValue);

		InetAddress realIP = HttpUtils.getRealIp(request);
		assertEquals(realIP, HttpUtils.inetAddress(forwardedOriginalAddrIPv6));
	}
}
