/**
 *
 *
 *
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

package kafka.metrics

import java.net.InetSocketAddress

import com.yammer.metrics.reporting._
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider
import kafka.server.KafkaServer
import kafka.utils.{Logging, VerifiableProperties}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer

private trait KafkaHttpMetricsReporterMBean extends KafkaMetricsReporterMBean

private class KafkaHttpMetricsReporter extends KafkaServerMetricsReporter
                              with KafkaHttpMetricsReporterMBean
                              with Logging {
  val defaultPort = 8080
  val defaultBindAddress = "localhost"

  private var metricsServer: Server = null

  private var initialized = false
  private var running = false

  override def getMBeanName = "kafka:type=kafka.metrics.KafkaHttpMetricsReporter"

  override def init(props: VerifiableProperties) { init(props, null) }

  override def init(props: VerifiableProperties, server: KafkaServer) {
    synchronized {
      if (!initialized) {
        val metricsConfig = new KafkaMetricsConfig(props)

        val bindAddress = props.getString("kafka.http.metrics.host", defaultBindAddress)
        val port = props.getInt("kafka.http.metrics.port", defaultPort)

        // creating the socket address for binding to the specified address and port
        val inetSocketAddress = new InetSocketAddress(bindAddress, port)

        // create new Jetty server
        metricsServer = new Server(inetSocketAddress)

        // creating the servlet context handler
        val handler = new ServletContextHandler()

        // Add a default 404 Servlet
        addMetricsServlet(handler, new DefaultServlet() with NoDoTrace, "/")

        // Add Metrics Servlets
        addMetricsServlet(handler, new MetricsServlet() with NoDoTrace, "/api/metrics")
        addMetricsServlet(handler, new ThreadDumpServlet() with NoDoTrace, "/api/threads")
        addMetricsServlet(handler, new PingServlet() with NoDoTrace, "/api/ping")

        // Add Custom Servlets
        val resourceConfig: ResourceConfig = new ResourceConfig
        resourceConfig.register(new JacksonJsonProvider(), 0)
        resourceConfig.register(new KafkaTopicsResource(server), 0) // TODO: NoDoTrace?
        resourceConfig.register()

        val servletContainer: ServletContainer = new ServletContainer(resourceConfig)
        val servletHolder: ServletHolder = new ServletHolder(servletContainer)
        handler.addServlet(servletHolder, "/api")

        // Add the handler to the server
        metricsServer.setHandler(handler)

        initialized = true
        startReporter(metricsConfig.pollingIntervalSecs)
      } else {
        error("Kafka Http Metrics Reporter already initialized")
      }
    }
  }

  private def addMetricsServlet(context: ServletContextHandler, servlet: HttpServlet, urlPattern: String): Unit = {
    context.addServlet(new ServletHolder(servlet), urlPattern)
  }

  private trait NoDoTrace extends HttpServlet {
    override def doTrace(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
      resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
    }
  }

  override def startReporter(pollingPeriodSecs: Long) {
    synchronized {
      if (initialized && !running) {
        metricsServer.start()
        running = true
        info(s"Started Kafka HTTP metrics reporter at ${metricsServer.getURI}")
      }
      else {
        if (running) {
          error("Kafka Http Metrics Reporter already running")
        }
      }
    }
  }

  override def stopReporter() {
    synchronized {
      if (initialized && running) {
        metricsServer.stop()
        info("Stopped Kafka CSV metrics reporter")
      }
    }
  }
}

