/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */

export default {
  // URL_PREFIX: '/gateway',
  // ADMIN_URL_PREFIX: '/admin',

  SCRATCH_VERSION: -1, // Definition being edited
  REQUEST_MESSAGE: 'request',
  RESPONSE_MESSAGE: 'response',
  DEVELOPMENT_MODE: true, // Should be set from config file
  DEFAULT_TENANT: 'datp', // Used to access formservice definitions

  BACKEND_REQUEST_MAPPING_SUFFIX: 'request',
  FRONTEND_RESPONSE_MAPPING_SUFFIX: 'response',
}

export const MONITOR_URL_PREFIX = '/mondat-api'
export const FORMSERVICE_URL_PREFIX = '/formservice'
export const DATP_URL_PREFIX = '/datp'
export const LOOKUP_URL_PREFIX = '/lookup'
