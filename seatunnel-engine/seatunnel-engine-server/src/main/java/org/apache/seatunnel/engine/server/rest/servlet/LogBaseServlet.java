/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.rest.servlet;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.common.utils.LogUtil;

import org.apache.commons.lang3.StringUtils;

import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServletResponse;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

@Slf4j
public class LogBaseServlet extends BaseServlet {

    public LogBaseServlet(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
    }

    protected String getLogParam(String uri, String contextPath) {
        uri = uri.substring(uri.indexOf(contextPath) + contextPath.length());
        uri = StringUtil.stripTrailingSlash(uri).substring(1);
        int indexEnd = uri.indexOf('/');
        if (indexEnd != -1) {
            String param = uri.substring(indexEnd + 1);
            return param;
        }
        return "";
    }

    /** Get configuration log path */
    protected String getLogPath() {
        try {
            return LogUtil.getLogPath();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.error("Get log path error", ExceptionUtils.getMessage(e));
            return null;
        }
    }

    protected String sendGet(String urlString) {
        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(urlString).openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.connect();

            if (connection.getResponseCode() == 200) {
                try (InputStream is = connection.getInputStream();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = is.read(buffer)) != -1) {
                        baos.write(buffer, 0, len);
                    }
                    return baos.toString();
                }
            }
        } catch (IOException e) {
            log.error("Send get Fail.", ExceptionUtils.getMessage(e));
        }
        return null;
    }

    protected String buildLogLink(String href, String name) {
        return "<li><a href=\"" + href + "\">" + name + "</a></li>\n";
    }

    protected String buildWebSiteContent(StringBuffer logLink) {
        return "<html><head><title>Seatunnel log</title></head>\n"
                + "<body>\n"
                + " <h2>Seatunnel log</h2>\n"
                + " <ul>\n"
                + logLink.toString()
                + " </ul>\n"
                + "</body></html>";
    }

    /** Prepare Log Response */
    protected void prepareLogResponse(HttpServletResponse resp, String logPath, String logName) {
        if (StringUtils.isBlank(logPath)) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            log.warn(
                    "Log file path is empty, no log file path configured in the current configuration file");
            return;
        }
        String logFilePath = logPath + "/" + logName;
        try {
            String logContent = FileUtils.readFileToStr(new File(logFilePath).toPath());
            write(resp, logContent);
        } catch (SeaTunnelRuntimeException | IOException e) {
            // If the log file does not exist, return 400
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            log.warn(String.format("Log file content is empty, get log path : %s", logFilePath));
        }
    }
}
