/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from ".."

/**
 * Provide an easy prefix for debug messages.
 * @returns Shortened version of the nodeId.
 */
export default function me() {
  const id = schedulerForThisNode.getNodeId()
  return id.substring(7, 13)
}