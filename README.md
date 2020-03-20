Siddhi IO File
======================================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-file/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-file/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-io-file.svg)](https://github.com/siddhi-io/siddhi-io-file/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-io-file.svg)](https://github.com/siddhi-io/siddhi-io-file/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-io-file.svg)](https://github.com/siddhi-io/siddhi-io-file/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-io-file.svg)](https://github.com/siddhi-io/siddhi-io-file/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-io-file extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> which used to receive/publish event data from/to file. It supports both binary and text formats.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 5.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.io.file/siddhi-io-file/">here</a>.
* Versions 4.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.io.file/siddhi-io-file">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8">2.0.8</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#isdirectory-function">isDirectory</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This function checks for a given file path points to a directory</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#isexist-function">isExist</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This function checks whether a file or a folder exists in a given path</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#isfile-function">isFile</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This function checks for a given file path points to a file</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#lastmodifiedtime-function">lastModifiedTime</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">Checks for the last modified time for a given file path</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#size-function">size</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This function checks for a given file's size</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#archive-stream-function">archive</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">Stream Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">Archives files and folders as a zip or in tar format that are available in the given file uri.<br></p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#copy-stream-function">copy</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">Stream Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This function performs copying file from one directory to another.<br></p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#create-stream-function">create</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">Stream Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">Create a file or a folder in the given location</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#delete-stream-function">delete</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">Stream Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">Deletes file/files in a particular path</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#move-stream-function">move</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">Stream Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This function performs copying file from one directory to another.<br></p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#search-stream-function">search</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">Stream Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">Searches files in a given folder and lists.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#searchinarchive-stream-function">searchInArchive</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">Stream Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#unarchive-stream-function">unarchive</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">Stream Function</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This function decompresses a given file</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#file-sink">file</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">Sink</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">File Sink can be used to publish (write) event data which is processed within siddhi to files. <br>Siddhi-io-file sink provides support to write both textual and binary data into files<br></p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-file/api/2.0.8/#file-source">file</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">Source</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">File Source provides the functionality for user to feed data to siddhi from files. Both text and binary files are supported by file source.</p></p></div>

## Dependencies 

There are no other dependencies needed for this extension.

## Installation

For installing this extension and to add the dependent jars on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions and jars</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

