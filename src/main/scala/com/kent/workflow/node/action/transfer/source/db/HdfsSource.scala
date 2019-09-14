package com.kent.workflow.node.action.transfer.source.db

import com.kent.pub.io.FileLink

class HdfsSource(fl: FileLink, delimited: String, path: String) extends GenericFileSource(fl, delimited, path) {

}