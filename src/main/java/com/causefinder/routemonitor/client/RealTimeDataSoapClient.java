package com.causefinder.routemonitor.client;

import com.causefinder.routemonitor.soap.model.StopData;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import javax.xml.soap.*;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class RealTimeDataSoapClient {

    public static void main(String args[]) {
        RealTimeDataSoapClient realTimeDataSoapClient = new RealTimeDataSoapClient();
        realTimeDataSoapClient.getRealTimeData(4096);
    }

    @Cacheable(cacheNames = "RealTimeStopDataCache", key = "T(java.util.Objects).hash(#p0)", cacheManager = "realTimeStopDataCacheManager")
    public List<StopData> getRealTimeData(Integer busStopId) {
        try {
            String soapEndpointUrl = "http://rtpi.dublinbus.ie/DublinBusRTPIService.asmx";
            SOAPConnectionFactory soapConnectionFactory = SOAPConnectionFactory.newInstance();
            SOAPConnection soapConnection = soapConnectionFactory.createConnection();
            SOAPMessage soapResponse = soapConnection.call(getRealTimeStopDataRequest(busStopId), soapEndpointUrl);

            // Print the SOAP Response
            // System.out.println(">>RTPI SOAP REQ SEND");
            // soapResponse.writeTo(System.out);
            // System.out.println();
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
            if (response.length() < 3519) {
                return new ArrayList<>();
            }
            String cleanedResponse = response.substring(3397, response.length() - 122);
            //System.out.println(cleanedResponse);

            XmlMapper xmlMapper = new XmlMapper();
            xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            StringBuilder cleanData = new StringBuilder(cleanedResponse);
            int locStart = 0;
            StopData stopData = null;
            List<StopData> stopDataList = new ArrayList<>();
            while (true) {
                locStart = cleanData.indexOf("<StopData diffgr");
                if (locStart == -1) break;
                int locStop = cleanData.indexOf(">");
                cleanData.replace(locStart, locStop + 1, "<StopData>");
                locStart = cleanData.indexOf("<StopData>");
                locStop = cleanData.indexOf("</StopData>");
                String element = cleanData.substring(locStart, locStop + 11);
                cleanData.replace(locStart, locStop + 11, "");
                stopData = xmlMapper.readValue(element, StopData.class);
                String dirStr = stopData.getDirection().equalsIgnoreCase("Inbound") ? "I" : "O";
                stopData.setDirection(dirStr);
                stopDataList.add(stopData);
            }
            return stopDataList;
        } catch (Exception e) {
            System.err.println("\nError occurred while sending SOAP Request to Server!\nMake sure you have the correct endpoint URL and SOAPAction!\n");
            e.printStackTrace();
        }
        return null;
    }


    private SOAPMessage getRealTimeStopDataRequest(Integer busStopId) throws Exception {
        MessageFactory messageFactory = MessageFactory.newInstance();
        SOAPMessage soapMessage = messageFactory.createMessage();

        SOAPPart soapPart = soapMessage.getSOAPPart();

        String dublinBusNamespace = "ns";
        String dublinBusNsUrl = "http://dublinbus.ie/";
        SOAPEnvelope envelope = soapPart.getEnvelope();
        envelope.addNamespaceDeclaration(dublinBusNamespace, dublinBusNsUrl);
        SOAPBody soapBody = envelope.getBody();

        String soapAction = "http://dublinbus.ie/GetRealTimeStopData";
        SOAPElement getRealTimeStopData = soapBody.addChildElement("GetRealTimeStopData", dublinBusNamespace);
        SOAPElement stopId = getRealTimeStopData.addChildElement("stopId", dublinBusNamespace);
        stopId.addTextNode(busStopId.toString());
        SOAPElement forceRefresh = getRealTimeStopData.addChildElement("forceRefresh", dublinBusNamespace);
        forceRefresh.addTextNode("true");
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