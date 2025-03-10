# API Docs - v2.0.25

!!! Info "Tested Siddhi Core version: *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/">5.1.26</a>*"
    It could also support other Siddhi Core minor versions.

## File

### isDirectory *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This function checks for a given file path points to a directory</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<BOOL> file:isDirectory(<STRING> uri)
<BOOL> file:isDirectory(<STRING> uri, <STRING> file.system.options)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The path to be checked for a directory.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
file:isDirectory(filePath) as isDirectory
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks whether the given path is a directory. Result will be returned as an boolean.</p>
<p></p>
### isExist *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This function checks whether a file or a folder exists in a given path</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<BOOL> file:isExist(<STRING> uri)
<BOOL> file:isExist(<STRING> uri, <STRING> file.system.options)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">File path to check for existence.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
file:isExist('/User/wso2/source/test.txt') as exists
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks existence of a file in the given path. Result will be returned as an boolean .</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
file:isExist('/User/wso2/source/') as exists
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks existence of a folder in the given path. Result will be returned as an boolean .</p>
<p></p>
### isFile *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This function checks for a given file path points to a file</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<BOOL> file:isFile(<STRING> file.path)
<BOOL> file:isFile(<STRING> file.path, <STRING> file.system.options)
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
        <td style="vertical-align: top">file.path</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The path to be checked for a file.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
file:isFile(filePath) as isFile
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks whether the given path is a file. Result will be returned as an boolean.</p>
<p></p>
### lastModifiedTime *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks for the last modified time for a given file path</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<STRING> file:lastModifiedTime(<STRING> uri)
<STRING> file:lastModifiedTime(<STRING> uri, <STRING> datetime.format)
<STRING> file:lastModifiedTime(<STRING> uri, <STRING> datetime.format, <STRING> file.system.options)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">File path to be checked for te last modified time.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">datetime.format</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Format of the last modified datetime to be returned.</p></td>
        <td style="vertical-align: top">MM/dd/yyyy HH:mm:ss</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
file:lastModifiedTime(filePath) as lastModifiedTime
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Last modified datetime of a file will be returned as an string in MM/dd/yyyy HH:mm:ss.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
file:lastModifiedTime(filePath, dd/MM/yyyy HH:mm:ss) as lastModifiedTime
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Last modified datetime of a file will be returned as an string in 'dd/MM/yyyy HH:mm:ss' format.</p>
<p></p>
### size *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This function checks for a given file's size</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<LONG> file:size(<STRING> uri)
<LONG> file:size(<STRING> uri, <STRING> file.system.options)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute path to the file or directory to be checked for the size.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
file:size('/User/wso2/source/test.txt') as fileSize
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Size of a file in a given path will be returned.</p>
<p></p>
### archive *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Archives files and folders as a zip or in tar format that are available in the given file uri.<br></p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
file:archive(<STRING> uri, <STRING> destination.dir.uri)
file:archive(<STRING> uri, <STRING> destination.dir.uri, <STRING> archive.type)
file:archive(<STRING> uri, <STRING> destination.dir.uri, <STRING> archive.type, <STRING> include.by.regexp)
file:archive(<STRING> uri, <STRING> destination.dir.uri, <STRING> archive.type, <STRING> include.by.regexp, <BOOL> exclude.subdirectories)
file:archive(<STRING> uri, <STRING> destination.dir.uri, <STRING> archive.type, <STRING> include.by.regexp, <BOOL> exclude.subdirectories, <STRING> file.system.options)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute path of the file or the directory</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">destination.dir.uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute directory path of the the archived file.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">archive.type</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Archive type can be zip or tar</p></td>
        <td style="vertical-align: top">zip</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">include.by.regexp</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Only the files matching the patterns will be archived.<br>Note: Add an empty string to match all files</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exclude.subdirectories</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This flag is used to exclude the subdirectories and its files without archiving.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
InputStream#file:archive('/User/wso2/to_be_archived', '/User/wso2/archive_destination/file.zip')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Archives to_be_archived folder in zip format and stores archive_destination folder as file.zip.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
InputStream#file:archive('/User/wso2/to_be_archived', '/User/wso2/archive_destination/file', 'tar')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Archives to_be_archived folder in tar format and stores in archive_destination folder as file.tar.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
InputStream#file:archive('/User/wso2/to_be_archived', '/User/wso2/archive_destination/file', 'tar', '.*test3.txt$')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Archives files which adheres to '.*test3.txt$' regex in to_be_archived folder in tar format and stores in archive_destination folder as file.tar.</p>
<p></p>
<span id="example-4" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 4</span>
```
InputStream#file:archive('/User/wso2/to_be_archived', '/User/wso2/archive_destination/file', '', '', 'false')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Archives to_be_archived folder excluding the sub-folders in zip format and stores in archive_destination folder as file.tar.</p>
<p></p>
### copy *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This function performs copying file from one directory to another.<br></p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
file:copy(<STRING> uri, <STRING> destination.dir.uri)
file:copy(<STRING> uri, <STRING> destination.dir.uri, <STRING> include.by.regexp)
file:copy(<STRING> uri, <STRING> destination.dir.uri, <STRING> include.by.regexp, <BOOL> exclude.root.dir)
file:copy(<STRING> uri, <STRING> destination.dir.uri, <STRING> include.by.regexp, <BOOL> exclude.root.dir, <STRING> file.system.options)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute path of the File or the directory.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">destination.dir.uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute path of the destination directory.<br>Note: Parent folder structure will be created if it does not exist.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">include.by.regexp</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Only the files matching the patterns will be copied.<br>Note: Add an empty string to match all files</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exclude.root.dir</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This flag is used to exclude parent folder when copying the content.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">isSuccess</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">Status of the file copying operation (true if success)</p></td>
        <td style="vertical-align: top">BOOL</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
InputStream#file:copy('/User/wso2/source/test.txt', 'User/wso2/destination/')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Copies 'test.txt' in 'source' folder to the 'destination' folder.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
InputStream#file:copy('/User/wso2/source/', 'User/wso2/destination/')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Copies 'source' folder to the 'destination' folder with all its content</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
InputStream#file:copy('/User/wso2/source/', 'User/wso2/destination/', '.*test3.txt$')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Copies 'source' folder to the 'destination' folder ignoring files doesnt adhere to the given regex.</p>
<p></p>
<span id="example-4" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 4</span>
```
InputStream#file:copy('/User/wso2/source/', 'User/wso2/destination/', '', true)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Copies only the files resides in 'source' folder to 'destination' folder.</p>
<p></p>
### create *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Create a file or a folder in the given location</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
file:create(<STRING> uri)
file:create(<STRING> uri, <BOOL> is.directory)
file:create(<STRING> uri, <BOOL> is.directory, <STRING> file.system.options)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute file path which needs to be created.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">is.directory</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This flag is used when creating file path is a directory</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from CreateFileStream#file:create('/User/wso2/source/test.txt', false)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Creates a file in the given path with the name of 'test.txt'.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from CreateFileStream#file:create('/User/wso2/source/', true)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Creates a folder in the given path with the name of 'source'.</p>
<p></p>
### delete *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Deletes file/files in a particular path</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
file:delete(<STRING> uri)
file:delete(<STRING> uri, <STRING> file.system.options)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute path of the file or the directory to be deleted.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from DeleteFileStream#file:delete('/User/wso2/source/test.txt')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Deletes the file in the given path. </p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from DeleteFileStream#file:delete('/User/wso2/source/')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Deletes the folder in the given path. </p>
<p></p>
### move *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This function performs copying file from one directory to another.<br></p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
file:move(<STRING> path, <STRING> destination.dir.path)
file:move(<STRING> path, <STRING> destination.dir.path, <STRING> include.by.regexp)
file:move(<STRING> path, <STRING> destination.dir.path, <STRING> include.by.regexp, <BOOL> exclude.root.dir)
file:move(<STRING> path, <STRING> destination.dir.path, <STRING> include.by.regexp, <BOOL> exclude.root.dir, <STRING> file.system.options)
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
        <td style="vertical-align: top">path</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute file or directory path.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">destination.dir.path</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute file path to the destination directory.<br>Note: Parent folder structure will be created if it does not exist.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">include.by.regexp</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Only the files matching the patterns will be moved.<br>Note: Add an empty string to match all files</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exclude.root.dir</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Exclude parent folder when moving the content.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">isSuccess</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">Status of the file moving operation (true if success)</p></td>
        <td style="vertical-align: top">BOOL</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
InputStream#file:move('/User/wso2/source/test.txt', 'User/wso2/destination/')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Moves 'test.txt' in 'source' folder to the 'destination' folder.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
InputStream#file:move('/User/wso2/source/', 'User/wso2/destination/')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Moves 'source' folder to the 'destination' folder with all its content</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
InputStream#file:move('/User/wso2/source/', 'User/wso2/destination/', '.*test3.txt$')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Moves 'source' folder to the 'destination' folder excluding files doesnt adhere to the given regex.</p>
<p></p>
<span id="example-4" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 4</span>
```
InputStream#file:move('/User/wso2/source/', 'User/wso2/destination/', '', true)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Moves only the files resides in 'source' folder to 'destination' folder.</p>
<p></p>
### rename *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This method can be used to rename a file/folder in a particular path, move a file from to a different path. <br>Ex- <br>&nbsp;file:rename('/User/wso2/source', 'User/wso2/destination') <br>&nbsp;file:rename('/User/wso2/source/file.csv', 'User/wso2/source/newFile.csv') <br>&nbsp;&nbsp;file:rename('/User/wso2/source/file.csv', 'User/wso2/destination/file.csv')</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
file:rename(<STRING> uri, <STRING> new.destination.name)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute path of the file or the directory to be rename.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">new.destination.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute path of the new file/folder</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:&lt;Realative path from '&lt;Product_Home&gt;/wso2/server/' directory&gt;,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">isSuccess</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">Status of the file rename operation (true if success)</p></td>
        <td style="vertical-align: top">BOOL</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
InputStream#file:rename('/User/wso2/source/', 'User/wso2/destination/')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Rename the file resides in 'source' folder to 'destination' folder.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
InputStream#file:rename('/User/wso2/folder/old.csv', 'User/wso2/folder/new.txt')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Rename 'old.csv' file resides in folder to 'new.txt'</p>
<p></p>
### search *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Searches files in a given folder and lists.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
file:search(<STRING> uri)
file:search(<STRING> uri, <STRING> include.by.regexp)
file:search(<STRING> uri, <STRING> include.by.regexp, <BOOL> exclude.subdirectories)
file:search(<STRING> uri, <STRING> include.by.regexp, <BOOL> exclude.subdirectories, <STRING> file.system.options)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute file path of the directory.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">include.by.regexp</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Only the files matching the patterns will be searched.<br>Note: Add an empty string to match all files</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exclude.subdirectories</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This flag is used to exclude the files un subdirectories when listing.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">fileNameList</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The lit file name matches in the directory.</p></td>
        <td style="vertical-align: top">OBJECT</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
ListFileStream#file:search(filePath)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will list all the files (also in sub-folders) in a given path.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
ListFileStream#file:search(filePath, '.*test3.txt$')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will list all the files (also in sub-folders) which adheres to a given regex file pattern in a given path.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
ListFileStream#file:search(filePath, '.*test3.txt$', true)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will list all the files excluding the files in sub-folders which adheres to a given regex file pattern in a given path.</p>
<p></p>
### searchInArchive *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
file:searchInArchive(<STRING> uri)
file:searchInArchive(<STRING> uri, <STRING> include.by.regexp)
file:searchInArchive(<STRING> uri, <STRING> include.by.regexp, <STRING> file.system.options)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute file path of the zip or tar file.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">include.by.regexp</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Only the files matching the patterns will be searched.<br>Note: Add an empty string to match all files</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>
<span id="extra-return-attributes" class="md-typeset" style="display: block; font-weight: bold;">Extra Return Attributes</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Possible Types</th>
    </tr>
    <tr>
        <td style="vertical-align: top">fileNameList</td>
        <td style="vertical-align: top;"><p style="word-wrap: break-word;margin: 0;">The list file names in the archived file.</p></td>
        <td style="vertical-align: top">OBJECT</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
ListArchivedFileStream#file:listFilesInArchive(filePath)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Lists the files inside the compressed file in the given path.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
ListArchivedFileStream#file:listFilesInArchive(filePath, '.*test3.txt$')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Filters file names adheres to the given regex and lists the files inside the compressed file in the given path.</p>
<p></p>
### unarchive *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This function decompresses a given file</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
file:unarchive(<STRING> uri, <STRING> destination.dir.uri)
file:unarchive(<STRING> uri, <STRING> destination.dir.uri, <BOOL> exclude.root.dir)
file:unarchive(<STRING> uri, <STRING> destination.dir.uri, <BOOL> exclude.root.dir, <STRING> file.system.options)
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
        <td style="vertical-align: top">uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute path of the file to be decompressed in the format of zip or tar.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">destination.dir.uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Absolute path of the destination directory.<br>Note: If the folder structure does not exist, it will be created.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">exclude.root.dir</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This flag excludes parent folder when extracting the content.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
file:unarchive('/User/wso2/source/test.zip', '/User/wso2/destination')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Unarchive a zip file in a given path to a given destination.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
file:unarchive('/User/wso2/source/test.tar', '/User/wso2/destination')
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Unarchive a tar file in a given path to a given destination.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
file:unarchive('/User/wso2/source/test.tar', '/User/wso2/destination', true)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Unarchive a tar file in a given path to a given destination excluding the root folder.</p>
<p></p>
## Sink

### file *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">(Sink)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">The File Sink component of the 'siddhi-io-fie' extension publishes (writes) event data that is processed within Siddhi to files. <br>Siddhi-io-file sink provides support to write both textual and binary data into files<br></p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@sink(type="file", file.uri="<STRING>", append="<BOOL>", add.line.separator="<BOOL>", file.system.options="<STRING>", @map(...)))
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
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The path to thee file in which the data needs to be published. </p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">append</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This specifies whether the data should be appended to the file or not.<br>If this parameter is set to 'true', data is written at the end of the file without changing the existing content.<br>&nbsp;If the parameter is set to 'false', the existing content of the file is deleted and the content you are publishing is added to replace it.<br>If the file does not exist, a new file is created and then the data is written in it. In such a scenario, the value specified for this parameter is not applicable<br></p></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">add.line.separator</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this parameter is set to 'true', events added to the file are separated by adding each event in a new line.<br></p></td>
        <td style="vertical-align: top">true. (However, if the 'csv' mapper is used, it is false)</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type='file', @map(type='json'), append='false', file.uri='/abc/{{symbol}}.txt') define stream BarStream (symbol string, price float, volume long); 
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In the above configuration, each output event is published in the '/abc/{{symbol}}.txt' file in JSON format.The output looks as follows:<br>{<br>&nbsp;&nbsp;&nbsp;&nbsp;"event":{<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"symbol":"WSO2",<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"price":55.6,<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"volume":100<br>&nbsp;&nbsp;&nbsp;&nbsp;}<br>}<br>If the file does not exist at the time an output event is generated, the system creates the file and proceeds to publish the output event in it.</p>
<p></p>
## Source

### file *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">(Source)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">The File Source component of the 'siddhi-io-fie' extension allows you to receive the input data to be processed by Siddhi via files. Both text files and binary files are supported.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@source(type="file", dir.uri="<STRING>", file.uri="<STRING>", mode="<STRING>", tailing="<BOOL>", action.after.process="<STRING>", action.after.failure="<STRING>", move.after.process="<STRING>", move.if.exist.mode="<STRING>", move.after.failure="<STRING>", begin.regex="<STRING>", end.regex="<STRING>", file.polling.interval="<STRING>", dir.polling.interval="<STRING>", timeout="<STRING>", file.read.wait.timeout="<STRING>", header.present="<BOOL>", header.line.count="<INT>", read.only.header="<BOOL>", read.only.trailer="<BOOL>", skip.trailer="<BOOL>", buffer.size="<STRING>", cron.expression="<STRING>", file.name.pattern="<STRING>", file.system.options="<STRING>", @map(...)))
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
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The path to the directory to be processed. During execution time, Siddhi by default processes all the files within this directory. However, if you have entered specific files to be processed via the 'file.name.list' parameter, only those files are processed. The URI specified must include the file handling protocol to be used for file processing.<br>e.g., If the file handling protocol to be used is 'ftp', the URI must be provided as 'ftp://&lt;DIRECTORY_PATH&gt;&gt;'.<br>At a given time, you should provide a value only for one out of the 'dir.uri' and 'file.uri' parameters. You can provide the directory URI if you have multiple files that you want to process within a directory. You can provide the file URI if you only need to process one file.</p></td>
        <td style="vertical-align: top">file:/var/tmp</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.uri</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The path to the file to be processed. The URI specified must include the file handling protocol to be used for file processing.<br>&nbsp;&nbsp;Only one of 'dir.uri' and 'file.uri' should be provided.<br>&nbsp;e.g., If the file handling protocol to be used is 'ftp', the URI must be provided as 'ftp://&lt;FILE_PATH&gt;&gt;'.<br>At a given time, you should provide a value only for one out of the 'dir.uri' and 'file.uri' parameters. You can provide the directory URI if you have multiple files that you want to process within a directory. You can provide the file URI if you only need to process one file.</p></td>
        <td style="vertical-align: top">file:/var/temp/tmp.text</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">mode</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This specifies the mode in which the files in given directory must be read.Possible values for this parameter are as follows:<br>- TEXT.FULL : to read a text file completely at once.<br>- BINARY.FULL : to read a binary file completely at once.<br>- BINARY.CHUNKED : to read a binary file chunk by chunk.<br>- LINE : to read a text file line by line.<br>- REGEX : to read a text file and extract data using a regex.<br></p></td>
        <td style="vertical-align: top">line</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">tailing</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this parameter is set to 'true', the file/the first file of the directory is tailed. <br>Do not set the parameter to 'true' and enable tailing if the mode is 'binary.full', 'text.full' or 'binary.chunked'.<br></p></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">action.after.process</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The action to be carried out after processing the file/directory. Possible values are 'DELETE' and 'MOVE'. 'DELETE' is default. If you specify 'MOVE', you need to specify a value for the 'move.after.process' parameter to indicate the location to which the consumed files should be moved.</p></td>
        <td style="vertical-align: top">delete</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">action.after.failure</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The action to be taken if a failure occurs while the file/directory is being processed. Possible values are 'DELETE' and 'MOVE'. 'DELETE' is default. If you specify 'MOVE', you need to specify a value for the 'move.after.failure' parameter to indicate the location to which the files that could not be read need to be moved</p></td>
        <td style="vertical-align: top">delete</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">move.after.process</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If you specify 'MOVE' as the value for the 'action.after.process' parameter, use this parameter to specify the location to which the consumed files need to be moved.This should be the absolute path of the file that is going to be created after the moving is done.<br>This URI must include the file handling protocol used for file processing.<br>e.g., If the file handling protocol is 'ftp', the URI must be provided as 'ftp://&lt;FILE_PATH&gt;&gt;'.</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">move.if.exist.mode</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If you specify 'MOVE' as the value for the 'action.after.process' parameter, use this parameter to specify what happens if a file exist in the same location.Possible values are 'OVERWRITE' and 'KEEP' where KEEP will append a UUID to existing filename and keep both files while OVERWRITE will simply overwrite the existing file.<br></p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">move.after.failure</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If you specify 'MOVE' as the value for the 'action.after.failure' parameter, use this parameter to specify the location to which the files should be moved after the failure<br>This should be the absolute path of the file that is going to be created after the failure.<br>This URI must include the file handling protocol used for file processing.<br>e.g., If the file handling protocol is 'ftp', the URI must be provided as 'ftp://&lt;FILE_PATH&gt;&gt;'.</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">begin.regex</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The regex to be matched at the beginning of the retrieved content.<br></p></td>
        <td style="vertical-align: top">None</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">end.regex</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The regex to be matched at the end of the retrieved content.<br></p></td>
        <td style="vertical-align: top">None</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.polling.interval</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The time interval (in milliseconds) of a polling cycle for a file.<br></p></td>
        <td style="vertical-align: top">1000</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">dir.polling.interval</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The time period (in milliseconds) of a polling cycle for a directory.<br></p></td>
        <td style="vertical-align: top">1000</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">timeout</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The maximum time duration (in milliseconds) that the system should wait until a file is processed.<br></p></td>
        <td style="vertical-align: top">5000</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.read.wait.timeout</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The maximum time duration (in milliseconds) that the system should wait before retrying to read the full file content.<br></p></td>
        <td style="vertical-align: top">1000</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">header.present</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If this parameter is set to 'true', it indicates the file(s) to be processed includes a header line(s). In such a scenario, the header line(s) are not processed. Number of header lines can be configured via 'header.line.count' parameter.<br></p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">header.line.count</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Number of lines to be considered as the file header. This parameter is applicable only if the parameter 'header.present' is set to 'true'.<br></p></td>
        <td style="vertical-align: top">1</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">read.only.header</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter is applicable only if the value for the 'mode' parameter is 'LINE'. If this parameter is set to 'true', only the first line (i.e., the header line) of a text file (e.g., CSV) is read. If it is set to 'false', the full content of the file is read line by line.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">read.only.trailer</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter is applicable only if the value for the 'mode' parameter is 'LINE'. If this parameter is set to 'true', only the last line (i.e., the trailer line) of a text file (e.g., CSV) is read. If it is set to 'false', the full content of the file is read line by line. This will only work if trailer appears once at the last line of file.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">skip.trailer</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter is applicable only if the value for the 'mode' parameter is 'LINE'. If this parameter is set to 'true', only the last line (i.e., the trailer line) of a text file (e.g., CSV) will be skipped. If it is set to 'false', the full content of the file is read line by line.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">buffer.size</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This parameter used to get the buffer size for binary.chunked mode.</p></td>
        <td style="vertical-align: top">65536</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">cron.expression</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is used to specify a timestamp in cron expression. The file or files in the given dir.uri or file.uri will be processed when the given expression satisfied by the system time.</p></td>
        <td style="vertical-align: top">None</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.name.pattern</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Regex pattern for the filenames that should be read from the directory. Note: This parameter is applicable only if the connector is reading from a directory</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
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
<p></p>
<p style="word-wrap: break-word;margin: 0;">In the above configuration, all the files in the given directory are picked and read one by one.<br>Here, it is assumed that all the files contain valid json strings with 'symbol', 'price', and 'volume' keys.<br>Once a file is read, its content is converted to events via the 'siddhi-map-json' extension. Those events are then received as input events in the the 'FooStream' stream.<br>Finally, after the reading is completed, the file is deleted.<br></p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@source(type='file',
mode='files.repo.line',
tailing='true',
dir.uri='file://abc/xyz',
@map(type='json')) 
define stream FooStream (symbol string, price float, volume long);
 
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In the above configuration, the first file in '/abc/xyz' directory is picked and read line by line.<br>Here, it is assumed that the file contains lines json strings.<br>For each line, the line content is converted to an event via the 'siddhi-map-json' extension. Those events are then received as input events in the the 'FooStream' stream.<br>Once the file content is completely read, the system keeps checking for new entries added to the file. If it detects a new entry, it immediately picks it up and processes it.<br></p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@source(type='file',
mode='text.full',
tailing='false'
 dir.uri='file://abc/xyz',
action.after.process='delete',
@map(type='csv' @attributes(eof = 'trp:eof', fp = 'trp:file.path'))) 
define stream FooStream (symbol string, price float, volume long); 

```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In the above configuration, all the files in the given directory are picked and read one by one.<br>Here, it is assumed that each file contains valid json strings with 'symbol', and 'price' keys.<br>Once a file is read, its content is converted to an event via the 'siddhi-map-json' extension with the additional 'eof' attribute. Then, that event is received as an input event in the 'FooStream' stream.<br>Once a file is completely read, it is deleted.<br></p>
<p></p>
### fileeventlistener *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">(Source)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">The 'fileeventlistener' component of the 'siddhi-io-fie' extension allows you to get the details of files that have been created, modified or deleted during execution time.Supports listening to local folder/file paths.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@source(type="fileeventlistener", dir.uri="<STRING>", monitoring.interval="<STRING>", file.name.list="<STRING>", file.system.options="<STRING>", @map(...)))
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
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The path to the directory to be processed. During execution time, Siddhi by default processes all the files within this directory. However, if you have entered specific files to be processed via the 'file.name.list' parameter, only those files are processed. The URI specified must include the file handling protocol to be used for file processing.<br></p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">monitoring.interval</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The time duration (in milliseconds) for which the system must monitor changes to the files in the specified directory.<br></p></td>
        <td style="vertical-align: top">100</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.name.list</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">If you want to carry out processing for only for one or more specific files in the the given directory URI, you can use this parameter to specify those files as a comma-separated list. <br>e.g., 'abc.txt,xyz.csv'</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">file.system.options</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The file options in key:value pairs separated by commas. <br>eg:'USER_DIR_IS_ROOT:false,PASSIVE_MODE:true,AVOID_PERMISSION_CHECK:true,IDENTITY:file://demo/.ssh/id_rsa,IDENTITY_PASS_PHRASE:wso2carbon'<br>Note: when IDENTITY is used, use a RSA PRIVATE KEY</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type='fileeventlistener', dir.uri='file://abc/xyz, file.name.list = 'xyz.txt, test') 
define stream FileListenerStream (filepath string, filename string, status string);
@sink(type='log')
define stream FooStream (filepath string, filename string, status string); 
from FileListenerStream
select *
insert into FooStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In the above configuration, the system monitors the given directory URI to check whether any file named either 'xyz.txt' or 'test' gets created, modified or deleted. If any such activity is detected, an input event is generated in the 'FooStream' stream. The information included in the event are the filepath, filename, and the status of the file.<br></p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@source(type='fileeventlistener',dir.uri='file://abc/xyz') 
define stream FileListenerStream (filepath string, filename string, status string);
@sink(type='log')
define stream FooStream (filepath string, filename string, status string); 
from FileListenerStream
select *
insert into FooStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In the above configuration, the system monitors the given directory URI to check whether any file gets created, modified or deleted. If any such activity is detected, an input event is generated in the 'FooStream' stream. The information included in the event are the filepath, filename, and the status of the file.<br></p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@source(type='fileeventlistener',dir.uri='file://abc/xyz', monitoring.interval='200')
define stream FileListenerStream (filepath string, filename string, status string);
@sink(type='log')
define stream FooStream (filepath string, filename string, status string);
from FileListenerStream
select *
insert into FooStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In the above configuration, the system monitors the given directory URI every 200 milliseconds to check whether any file gets created, modified or deleted. If any such activity is detected, an input event is generated in the 'FooStream' stream. The information included in the event are the filepath, filename, and the status of the file.<br></p>
<p></p>
