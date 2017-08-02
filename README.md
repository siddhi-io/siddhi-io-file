siddhi-io-http
======================================
---
|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-io-file/badge/icon)](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-io-file/) |
---
##### New version of Siddhi v4.0.0 is built in Java 8.

This repository contains source code for siddhi-io-file extension. 
Siddhi-io-file is used to receive/publish event data from/to file. It supports both binary and text formats.

This repository can be independently released from Siddhi.

Features Supported
------------------
 - File source
    - Multiple sources can be defined.
    - Enables WSO2 Stream Processor to receive messages from files.
 - File sink 
   - Multiple sinks can be defined.
   - Enables WSO2 Stream Processor to publish messages to files.
 - File tailing capability
   - Enable Stream Processor to tail a specific file.
     
Prerequisites for using the feature
------------------
 - Siddhi Stream should be defined
 - Stream processor should be running. 
  
Deploying the feature
------------------
 Feature can be deploy as a OSGI bundle by putting jar file of component to Wso2SPHome/lib directory of SP 4.0.0 pack. 
 
Example Siddhi Queries
------------------ 
#### Event Source
##### Reading all the json files in a directory completely which contain single json messages.
 
     @source(
            type='file', 
            dir.uri='abc/def/',
            mode = 'text.full',
            @map(type='json')
      )
     define stream inputStream (symbol string, price double, volume long);
     
##### Reading a single json file completely which contains a single json message.
 
     @source(
            type='file', 
            dir.uri='abc/def/message.json',
            mode = 'text.full',
            @map(type='json')
      )
     define stream inputStream (symbol string, price double, volume long);
     
##### Reading all the json files in a directory line by line without tailing enabled.
 
     @source(
            type='file', 
            dir.uri='abc/def/',
            mode = 'line',
            tailing='false',
            @map(type='json')
      )
     define stream inputStream (symbol string, price double, volume long);

##### Reading a single json file line by line without tailing enabled.
 
     @source(
            type='file', 
            dir.uri='abc/def/file.json',
            mode = 'line',
            tailing='false',
            @map(type='json')
      )
     define stream inputStream (symbol string, price double, volume long);
       
##### Reading a single json file line by line with tailing enabled.
 
     @source(
            type='file', 
            dir.uri='abc/def/file.json',
            mode = 'line',
            tailing='true',
            @map(type='json')
      )
     define stream inputStream (symbol string, price double, volume long);
 
   or
   
     @source(
            type='file', 
            dir.uri='abc/def/file.json',
            mode = 'line',
            @map(type='json')
      )
     define stream inputStream (symbol string, price double, volume long);
     
##### Reading the first file in a directory which is sorted by name in ascending order, line by line with tailing enabled.
 
     @source(
            type='file', 
            dir.uri='abc/def/',
            mode = 'line',
            tailing='true',
            @map(type='json')
      )
     define stream inputStream (symbol string, price double, volume long);
     
   or

     @source(
            type='file', 
            dir.uri='abc/def/',
            mode = 'line',
            @map(type='json')
      )
     define stream inputStream (symbol string, price double, volume long);

##### Reading all the files in a directory which contain xml messages using regular expressions.
 
     @source(
            type='file', 
            dir.uri='abc/def/',
            mode = 'regex',
            begin.regex = '(<events>)',
            end.regex =  '(</evnets>))',
            tailing='false',
            @map(type='xml')
      )
     define stream inputStream (symbol string, price double, volume long);

##### Reading a single file in a directory which contains xml messages using regular expressions without tailing enabled.
 
     @source(
            type='file', 
            dir.uri='abc/def/xmlMsgs.xml',
            mode = 'regex',
            begin.regex = '(<events>)',
            end.regex =  '(</evnets>))',
            tailing='false',
            @map(type='xml')
      )
     define stream inputStream (symbol string, price double, volume long);
     
##### Reading a single file which contains xml messages using regular expressions with tailing enabled.
 
     @source(
            type='file', 
            dir.uri='abc/def/xmlMsgs.xml',
            mode = 'regex',
            begin.regex = '(<events>)',
            end.regex =  '(</evnets>))',
            tailing='true',
            @map(type='xml')
      )
     define stream inputStream (symbol string, price double, volume long);
     
##### Reading the first file in a directory which is sorted by name in ascending order, using regular expressions with tailing enabled.
 
     @source(
            type='file', 
            dir.uri='abc/def/',
            mode = 'regex',
            begin.regex = '(<events>)',
            end.regex =  '(</evnets>))',
            tailing='true',
            @map(type='xml')
      )
     define stream inputStream (symbol string, price double, volume long);
     
   or

     @source(
            type='file', 
            dir.uri='abc/def/',
            mode = 'regex',
            begin.regex = '(<events>)',
            end.regex =  '(</evnets>))',
            @map(type='xml')
      )
     define stream inputStream (symbol string, price double, volume long);

#### Event Sink

##### Publishing event data as json messages to a new single specific file.
 
     @sink(
            type='file',
            file.uri='/abc/def/jsonMsgs.json',
            append='false', 
            @map(type='json')
     )
     define stream (symbol string, price double, volume long);
     
##### Publishing event data as json messages to an existing single specific file by appending.
 
     @sink(
            type='file',
            file.uri='/abc/def/jsonMsgs.json',
            append='true', 
            @map(type='json')
     )
     define stream (symbol string, price double, volume long);
     
##### Publishing event data as json messages to new files with dynamic names.
 
     @sink(
            type='file',
            file.uri='/abc/def/{{symbol}}.json',
            append='false', 
            @map(type='json')
     )
     define stream (symbol string, price double, volume long);
     
##### Publishing event data as json messages to files with dynamic names by appending.
 
     @sink(
            type='file',
            file.uri='/abc/def/{{symbol}}.json',
            append='true', 
            @map(type='json')
     )
     define stream (symbol string, price double, volume long);

Documentation 
------------------
  * https://docs.wso2.com/display/SP400/Configuring+File+Event+Sources
  * https://docs.wso2.com/display/SP400/Configuring+File+Event+Sinks

How to Contribute
------------------
* Send your bug fixes pull requests to [master branch] (https://github.com/wso2-extensions/siddhi-io-file/tree/master) 

Contact us 
----------
Siddhi developers can be contacted via the mailing lists:
  * Carbon Developers List : dev@wso2.org
  * Carbon Architecture List : architecture@wso2.org



WSO2 Smart Analytics Team.
