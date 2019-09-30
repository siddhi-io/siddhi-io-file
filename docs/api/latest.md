# API Docs - v2.0.2-SNAPSHOT

## Sink

### file *<a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#sink">(Sink)</a>*

<p style="word-wrap: break-word">File Sink can be used to publish (write) event data which is processed within siddhi to files. <br>Siddhi-io-file sink provides support to write both textual and binary data into files<br></p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@sink(type="file", file.uri="<STRING>", append="<BOOL>", add.line.separator="<BOOL>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">file.uri</td>
        <td style="vertical-align: top; word-wrap: break-word">Used to specify the file for data to be written. </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">append</td>
        <td style="vertical-align: top; word-wrap: break-word">This parameter is used to specify whether the data should be append to the file or not.<br>If append = 'true', data will be write at the end of the file without changing the existing content.<br>If file does not exist, a new fill will be crated and then data will be written.<br>If append append = 'false', <br>If given file exists, existing content will be deleted and then data will be written back to the file.<br>If given file does not exist, a new file will be created and then data will be written on it.<br></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">add.line.separator</td>
        <td style="vertical-align: top; word-wrap: break-word">This parameter is used to specify whether events added to the file should be separated by a newline.<br>If add.event.separator= 'true',then a newline will be added after data is added to the file.</td>
        <td style="vertical-align: top">true. (However, if csv mapper is used, it is false)</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type='file', @map(type='json'), append='false', file.uri='/abc/{{symbol}}.txt') define stream BarStream (symbol string, price float, volume long); 
```
<p style="word-wrap: break-word">Under above configuration, for each event, a file will be generated if there's no such a file,and then data will be written to that file as json messagesoutput will looks like below.<br>{<br>&nbsp;&nbsp;&nbsp;&nbsp;"event":{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"symbol":"WSO2",<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"price":55.6,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"volume":100<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>}<br></p>

## Source

### file *<a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#source">(Source)</a>*

<p style="word-wrap: break-word">File Source provides the functionality for user to feed data to siddhi from files. Both text and binary files are supported by file source.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(type="file", dir.uri="<STRING>", file.uri="<STRING>", mode="<STRING>", tailing="<BOOL>", action.after.process="<STRING>", action.after.failure="<STRING>", move.after.process="<STRING>", move.after.failure="<STRING>", begin.regex="<STRING>", end.regex="<STRING>", file.polling.interval="<STRING>", dir.polling.interval="<STRING>", timeout="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">dir.uri</td>
        <td style="vertical-align: top; word-wrap: break-word">Used to specify a directory to be processed. <br>All the files inside this directory will be processed. <br>Only one of 'dir.uri' and 'file.uri' should be provided.<br>This uri MUST have the respective protocol specified.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.uri</td>
        <td style="vertical-align: top; word-wrap: break-word">Used to specify a file to be processed. <br>&nbsp;Only one of 'dir.uri' and 'file.uri' should be provided.<br>This uri MUST have the respective protocol specified.<br></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">mode</td>
        <td style="vertical-align: top; word-wrap: break-word">This parameter is used to specify how files in given directory should.Possible values for this parameter are,<br>1. TEXT.FULL : to read a text file completely at once.<br>2. BINARY.FULL : to read a binary file completely at once.<br>3. LINE : to read a text file line by line.<br>4. REGEX : to read a text file and extract data using a regex.<br></td>
        <td style="vertical-align: top">line</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tailing</td>
        <td style="vertical-align: top; word-wrap: break-word">This can either have value true or false. By default it will be true. <br>This attribute allows user to specify whether the file should be tailed or not. <br>If tailing is enabled, the first file of the directory will be tailed.<br>Also tailing should not be enabled in 'binary.full' or 'text.full' modes.<br></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">action.after.process</td>
        <td style="vertical-align: top; word-wrap: break-word">This parameter is used to specify the action which should be carried out <br>after processing a file in the given directory. <br>It can be either DELETE or MOVE and default value will be 'DELETE'.<br>If the action.after.process is MOVE, user must specify the location to move consumed files using 'move.after.process' parameter.<br></td>
        <td style="vertical-align: top">delete</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">action.after.failure</td>
        <td style="vertical-align: top; word-wrap: break-word">This parameter is used to specify the action which should be carried out if a failure occurred during the process. <br>It can be either DELETE or MOVE and default value will be 'DELETE'.<br>If the action.after.failure is MOVE, user must specify the location to move consumed files using 'move.after.failure' parameter.<br></td>
        <td style="vertical-align: top">delete</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">move.after.process</td>
        <td style="vertical-align: top; word-wrap: break-word">If action.after.process is MOVE, user must specify the location to move consumed files using 'move.after.process' parameter.<br>This should be the absolute path of the file that going to be created after moving is done.<br>This uri MUST have the respective protocol specified.<br></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">move.after.failure</td>
        <td style="vertical-align: top; word-wrap: break-word">If action.after.failure is MOVE, user must specify the location to move consumed files using 'move.after.failure' parameter.<br>This should be the absolute path of the file that going to be created after moving is done.<br>This uri MUST have the respective protocol specified.<br></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">begin.regex</td>
        <td style="vertical-align: top; word-wrap: break-word">This will define the regex to be matched at the beginning of the retrieved content.<br></td>
        <td style="vertical-align: top">None</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">end.regex</td>
        <td style="vertical-align: top; word-wrap: break-word">This will define the regex to be matched at the end of the retrieved content.<br></td>
        <td style="vertical-align: top">None</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.polling.interval</td>
        <td style="vertical-align: top; word-wrap: break-word">This parameter is used to specify the time period (in milliseconds) of a polling cycle for a file.<br></td>
        <td style="vertical-align: top">1000</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">dir.polling.interval</td>
        <td style="vertical-align: top; word-wrap: break-word">This parameter is used to specify the time period (in milliseconds) of a polling cycle for a directory.<br></td>
        <td style="vertical-align: top">1000</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">timeout</td>
        <td style="vertical-align: top; word-wrap: break-word">This parameter is used to specify the maximum time period (in milliseconds)  for waiting until a file is processed.<br></td>
        <td style="vertical-align: top">5000</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type='file',
mode='text.full',
tailing='false'
 dir.uri='file://abc/xyz',
action.after.process='delete',
@map(type='json')) 
define stream FooStream (symbol string, price float, volume long); 

```
<p style="word-wrap: break-word">Under above configuration, all the files in directory will be picked and read one by one.<br>In this case, it's assumed that all the files contains json valid json strings with keys 'symbol','price' and 'volume'.<br>Once a file is read, its content will be converted to an event using siddhi-map-json extension and then, that event will be received to the FooStream.<br>Finally, after reading is finished, the file will be deleted.<br></p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@source(type='file',
mode='files.repo.line',
tailing='true',
dir.uri='file://abc/xyz',
@map(type='json')) 
define stream FooStream (symbol string, price float, volume long);
 
```
<p style="word-wrap: break-word">Under above configuration, the first file in directory '/abc/xyz'  will be picked and read line by line.<br>In this case, it is assumed that the file contains lines json strings.<br>For each line, line content will be converted to an event using siddhi-map-json extension and then, that event will be received to the FooStream.<br>Once file content is completely read, it will keep checking whether a new entry is added to the file or not.<br>If such entry is added, it will be immediately picked up and processed.<br></p>

