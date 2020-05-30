package com.causefinder.routemonitor.client;

import com.causefinder.routemonitor.soap.model.Stops;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import javax.xml.soap.*;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class StopDataByRouteAndDirectionSoapClient {

    public static void main(String args[]) {
        StopDataByRouteAndDirectionSoapClient realTimeDataSoapClient = new StopDataByRouteAndDirectionSoapClient();
        realTimeDataSoapClient.getStopDataByRouteAndDirection("44", "I");
    }

    @Cacheable(cacheNames = "RouteDataCache", key = "T(java.util.Objects).hash(#p0,#p1)")
    public List<Stops> getStopDataByRouteAndDirection(String route, String direction) {
        try {
            String soapEndpointUrl = "http://rtpi.dublinbus.ie/DublinBusRTPIService.asmx";
            SOAPConnectionFactory soapConnectionFactory = SOAPConnectionFactory.newInstance();
            SOAPConnection soapConnection = soapConnectionFactory.createConnection();
            SOAPMessage soapResponse = soapConnection.call(getStopDataByRouteAndDirectionRequest(route, direction), soapEndpointUrl);

            //Print the SOAP Response
            //System.out.println("Calling SOAP StopDataByRouteAndDirection API");
            //soapResponse.writeTo(System.out);
            //System.out.println();
            OutputStream output = new OutputStream() {
                private StringBuilder string = new StringBuilder();

                @Override
                public void write(int b) throws IOException {
                    this.string.append((char) b);
                }

                public String toString() {
                    return this.string.toString();
                }
            };
            soapResponse.writeTo(output);
            soapConnection.close();
            String response = output.toString();

            if (response.length() < 1600) {
                return new ArrayList<>();
            }
            String cleanedResponse = response.substring(1483, response.length() - 144);
            // System.out.println(cleanedResponse);
            XmlMapper xmlMapper = new XmlMapper();
            xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            StringBuilder cleanData = new StringBuilder(cleanedResponse);
            int locStart = 0;
            Stops stopData = null;
            List<Stops> stopDataList = new ArrayList<>();
            while (true) {
                locStart = cleanData.indexOf("<Stops diffgr");
                if (locStart == -1) break;
                int locStop = cleanData.indexOf(">");
                cleanData.replace(locStart, locStop + 1, "<Stops>");
                locStart = cleanData.indexOf("<Stops>");
                locStop = cleanData.indexOf("</Stops>");
                String element = cleanData.substring(locStart, locStop + 8);
                cleanData.replace(locStart, locStop + 8, "");
                stopData = xmlMapper.readValue(element, Stops.class);
                stopDataList.add(stopData);
            }
            Collections.sort(stopDataList);
            return stopDataList;
        } catch (Exception e) {
            System.err.println("\nError occurred while sending SOAP Request to Server!\nMake sure you have the correct endpoint URL and SOAPAction!\n");
            e.printStackTrace();
        }
        return null;
    }


    private SOAPMessage getStopDataByRouteAndDirectionRequest(String route, String direction) throws Exception {
        MessageFactory messageFactory = MessageFactory.newInstance();
        SOAPMessage soapMessage = messageFactory.createMessage();

        SOAPPart soapPart = soapMessage.getSOAPPart();

        String dublinBusNamespace = "ns";
        String dublinBusNsUrl = "http://dublinbus.ie/";
        SOAPEnvelope envelope = soapPart.getEnvelope();
        envelope.addNamespaceDeclaration(dublinBusNamespace, dublinBusNsUrl);
        SOAPBody soapBody = envelope.getBody();

        String soapAction = "http://dublinbus.ie/GetStopDataByRouteAndDirection";
        SOAPElement getRealTimeStopData = soapBody.addChildElement("GetStopDataByRouteAndDirection", dublinBusNamespace);
        SOAPElement stopId = getRealTimeStopData.addChildElement("route", dublinBusNamespace);
        stopId.addTextNode(route);
        SOAPElement forceRefresh = getRealTimeStopData.addChildElement("direction", dublinBusNamespace);
        forceRefresh.addTextNode(direction);
        MimeHeaders headers = soapMessage.getMimeHeaders();
        headers.addHeader("SOAPAction", soapAction);
        soapMessage.saveChanges();

        /* Print the request message, just for debugging purposes */
        // System.out.println("Request SOAP Message:");
        // soapMessage.writeTo(System.out);
        //System.out.println("\n");

        return soapMessage;
    }

}