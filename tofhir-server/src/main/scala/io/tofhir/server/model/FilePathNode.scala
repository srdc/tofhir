package io.tofhir.server.model

/**
 * Represents a node for each file in the file system
 * @param label The name of the file
 * @param isFolder True if the file is a folder
 * @param children The children of the folder
 * @param path file path of the node
 */
case class FilePathNode(label: String, isFolder: Boolean, children: List[FilePathNode], path: String)
