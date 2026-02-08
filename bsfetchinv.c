/* -----------------------------------------------------------------------------
 *
 * Copyright (c) 2014-2019 Alexis Naveros.
 *
 * This software is provided 'as-is', without any express or implied
 * warranty. In no event will the authors be held liable for any damages
 * arising from the use of this software.
 *
 * Permission is granted to anyone to use this software for any purpose,
 * including commercial applications, and to alter it and redistribute it
 * freely, subject to the following restrictions:
 *
 * 1. The origin of this software must not be misrepresented; you must not
 * claim that you wrote the original software. If you use this software
 * in a product, an acknowledgment in the product documentation would be
 * appreciated but is not required.
 * 2. Altered source versions must be plainly marked as such, and must not be
 * misrepresented as being the original software.
 * 3. This notice may not be removed or altered from any source distribution.
 *
 * -----------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include "cpuconfig.h"
#include "cc.h"
#include "ccstr.h"
#include "mm.h"
#include "mmatomic.h"
#include "mmbitmap.h"
#include "iolog.h"
#include "debugtrack.h"
#include "rand.h"

#if CC_UNIX
 #include <sys/types.h>
 #include <sys/stat.h>
 #include <unistd.h>
#elif CC_WINDOWS
 #include <windows.h>
 #include <direct.h>
#else
 #error Unknown/Unsupported platform!
#endif

#include "tcp.h"
#include "tcphttp.h"
#include "oauth.h"
#include "exclperm.h"
#include "journal.h"

#include "bsx.h"
#include "bsxpg.h"
#include "json.h"
#include "bsorder.h"
#include "bricklink.h"
#include "brickowl.h"
#include "brickowlinv.h"
#include "colortable.h"
#include "bstranslation.h"

#include "bricksync.h"


////


static void bsBrickLinkReplyInventory( void *uservalue, int resultcode, httpResponse *response )
{
  bsContext *context;
  bsQueryReply *reply;
  bsxInventory *inv;

  DEBUG_SET_TRACKER();

  reply = uservalue;
  context = reply->context;

#if 0
  ccFileStore( "zzz.txt", data, datasize, 0 );
#endif

  reply->result = resultcode;
  if( ( response ) && ( response->httpcode != 200 ) )
  {
    if( response->httpcode )
      reply->result = HTTP_RESULT_CODE_ERROR;
    bsStoreError( context, "BrickLink HTTP Error", response->header, response->headerlength, response->body, response->bodysize );
  }
  mmListDualAddLast( &context->replylist, reply, offsetof(bsQueryReply,list) );

  /* Parse inventory right here */
  inv = (bsxInventory *)reply->opaquepointer;
  if( ( reply->result == HTTP_RESULT_SUCCESS ) && ( response->body ) )
  {
    if( !( blReadInventory( inv, (char *)response->body, &context->output ) ) )
    {
      reply->result = HTTP_RESULT_PARSE_ERROR;
      bsStoreError( context, "BrickLink JSON Parse Error", response->header, response->headerlength, response->body, response->bodysize );
    }
  }

  return;
}


/* Query the inventory from BrickLink */
bsxInventory *bsQueryBrickLinkInventory( bsContext *context )
{
  bsQueryReply *reply;
  bsxInventory *inv;
  bsTracker tracker;

  DEBUG_SET_TRACKER();

  bsTrackerInit( &tracker, context->bricklink.http );
  inv = bsxNewInventory();
  ioPrintf( &context->output, IO_MODEBIT_FLUSH, BSMSG_INFO "Fetching the BrickLink Inventory...\n" );
  for( ; ; )
  {
    /* Add an Inventory query */
    reply = bsAllocReply( context, BS_QUERY_TYPE_BRICKLINK, 0, 0, (void *)inv );
#if 1
    /* Only available inventory */
    bsBrickLinkAddQuery( context, "GET", "/api/store/v1/inventories", "status=Y", 0, (void *)reply, bsBrickLinkReplyInventory );
#else
    /* Available + stockroom? */
    bsBrickLinkAddQuery( context, "GET", "/api/store/v1/inventories", "status=Y%2CS", 0, (void *)reply, bsBrickLinkReplyInventory );
#endif
    /* Wait until all queries are processed */
    bsWaitBrickLinkQueries( context, 0 );
    /* Free all queued replies, break on success */
    if( bsTrackerProcessGenericReplies( context, &tracker, 1 ) )
      break;
    if( tracker.failureflag )
    {
      bsxFreeInventory( inv );
      return 0;
    }
  }

  return inv;
}


////
// BrickStore-style inventory fallback (access-token -> sessionToken -> invExcelFinal.asp)
//
// Motivation: BrickLink's public API returns an empty inventory when the store is "closed",
// but BrickStore can still download inventory via the authenticated web endpoints.
// This fallback only kicks in when the API inventory appears empty AND a BrickStore access token is configured.
////

static int bsBrickLinkParseSessionToken( const char *body, size_t bodysize, char **ret_sessiontoken )
{
  const char *p, *end, *q;

  *ret_sessiontoken = 0;
  if( !( body ) || !( bodysize ) )
    return 0;

  end = body + bodysize;

  /* Find "sessionToken" */
  p = body;
  for( ; ; )
  {
    p = strstr( p, "\"sessionToken\"" );
    if( !( p ) )
      return 0;
    q = p + 13;
    if( q < end )
      break;
    return 0;
  }

  /* Find the next ':' then the opening quote */
  p = strchr( q, ':' );
  if( !( p ) || ( p >= end ) )
    return 0;
  p++;
  while( ( p < end ) && ( (unsigned char)*p <= ' ' ) )
    p++;
  if( ( p >= end ) || ( *p != '\"' ) )
    return 0;
  p++;
  q = p;
  while( ( q < end ) && ( *q != '\"' ) )
    q++;
  if( q >= end )
    return 0;

  /* Allocate token */
  {
    size_t len = (size_t)( q - p );
    char *t = malloc( len + 1 );
    memcpy( t, p, len );
    t[len] = 0;
    *ret_sessiontoken = t;
  }

  return 1;
}

static void bsBrickLinkReplyBrickStoreAuth( void *uservalue, int resultcode, httpResponse *response )
{
  bsContext *context;
  bsQueryReply *reply;
  char *sessiontoken;

  DEBUG_SET_TRACKER();

  reply = uservalue;
  context = reply->context;

  reply->result = resultcode;
  if( ( response ) && ( response->httpcode != 200 ) )
  {
    if( response->httpcode )
      reply->result = HTTP_RESULT_CODE_ERROR;
    bsStoreError( context, "BrickLink BrickStore-Auth HTTP Error", response->header, response->headerlength, response->body, response->bodysize );
  }

  /* Parse session token right here */
  if( ( reply->result == HTTP_RESULT_SUCCESS ) && ( response ) && ( response->body ) )
  {
    sessiontoken = 0;
    if( bsBrickLinkParseSessionToken( (const char *)response->body, response->bodysize, &sessiontoken ) )
    {
      if( context->bricklink.sessiontoken )
        free( context->bricklink.sessiontoken );
      context->bricklink.sessiontoken = sessiontoken;
    }
    else
    {
      reply->result = HTTP_RESULT_PARSE_ERROR;
      bsStoreError( context, "BrickLink BrickStore-Auth Parse Error", response->header, response->headerlength, response->body, response->bodysize );
    }
  }

  mmListDualAddLast( &context->replylist, reply, offsetof(bsQueryReply,list) );
  return;
}

static int bsBrickLinkBrickStoreAuthenticate( bsContext *context )
{
  bsQueryReply *reply;
  bsTracker tracker;
  char *jsonbody;
  char *querystring;
  int bodylen;

  DEBUG_SET_TRACKER();

  if( !( context->bricklink.brickstoretoken ) )
    return 0;
  if( !( context->bricklink.accounthttp ) )
    return 0;

  bsTrackerInit( &tracker, context->bricklink.accounthttp );

  /* Body: {"clientId":"<uuid>","clientToken":"<token>"} */
  jsonbody = ccStrAllocPrintf( "{\"clientId\":\"%s\",\"clientToken\":\"%s\"}", BS_BRICKLINK_TPA_CLIENT_ID, context->bricklink.brickstoretoken );
  bodylen = (int)strlen( jsonbody );

  for( ; ; )
  {
    ioPrintf( &context->output, IO_MODEBIT_FLUSH, BSMSG_DEBUG "BrickStore auth: requesting BrickLink session token...\n" );

    querystring = ccStrAllocPrintf(
      "POST /api/v1/actions/verify-and-create-session HTTP/1.1\r\n"
      "Host: %s\r\n"
      "Connection: Keep-Alive\r\n"
      "Content-Type: application/json\r\n"
      "x-bl-tpa-client-id: %s\r\n"
      "Content-Length: %d\r\n"
      "\r\n"
      "%s",
      BS_BRICKLINK_ACCOUNT_SERVER, BS_BRICKLINK_TPA_CLIENT_ID, bodylen, jsonbody
    );

    reply = bsAllocReply( context, BS_QUERY_TYPE_OTHER, 0, 0, 0 );
    httpAddQuery( context->bricklink.accounthttp, querystring, ccStrlen( querystring ), HTTP_QUERY_FLAGS_RETRY, (void *)reply, bsBrickLinkReplyBrickStoreAuth );
    free( querystring );

    /* Wait until all queries are processed */
    for( ; ; )
    {
      bsFlushTcpProcessHttp( context );
      if( httpGetQueryQueueCount( context->bricklink.accounthttp ) > 0 )
        tcpWait( &context->tcp, 0 );
      else
        break;
    }

    if( bsTrackerProcessGenericReplies( context, &tracker, 1 ) )
      break;
    if( tracker.failureflag )
    {
      free( jsonbody );
      return 0;
    }
  }

  free( jsonbody );

  if( !( context->bricklink.sessiontoken ) )
    return 0;
  return 1;
}


static char *bsBrickLinkXmlReadString( char **readvalue, char *string, const char *close )
{
  char *end;
  if( !( string ) )
    return 0;
  end = ccStrFindStr( string, (char *)close );
  if( !( end ) )
  {
    *readvalue = 0;
    return 0;
  }
  if( end == string )
    *readvalue = 0;
  else
  {
    *readvalue = string;
    end[0] = 0;
  }
  return end + strlen( close );
}

static int bsBrickLinkParseStoreInventoryXml( bsxInventory *inv, const void *body, size_t bodysize, ioLog *log )
{
  char *buf, *p, *itemstart, *itemend, *s;
  bsxItem item;
  char *decoded;
  int decodedlen;

  (void)log;

  if( !( body ) || !( bodysize ) )
    return 0;

  buf = malloc( bodysize + 1 );
  memcpy( buf, body, bodysize );
  buf[bodysize] = 0;

  p = buf;
  for( ; ; )
  {
    itemstart = ccStrFindStrSkip( p, "<ITEM>" );
    if( !( itemstart ) )
      break;
    itemend = ccStrFindStrSkip( itemstart, "</ITEM>" );
    if( !( itemend ) )
      break;

    bsxClearItem( &item );

    /* Simple fields */
    if( ( s = ccStrFindStrSkip( itemstart, "<ITEMID>" ) ) )
    {
      if( bsBrickLinkXmlReadString( &item.id, s, "</ITEMID>" ) )
        ;
    }
    if( ( s = ccStrFindStrSkip( itemstart, "<ITEMTYPE>" ) ) )
    {
      char *v = 0;
      if( bsBrickLinkXmlReadString( &v, s, "</ITEMTYPE>" ) )
      {
        if( ( v ) && ( v[0] ) )
          item.typeid = v[0];
      }
    }
    if( ( s = ccStrFindStrSkip( itemstart, "<COLOR>" ) ) )
    {
      char *v = 0;
      if( bsBrickLinkXmlReadString( &v, s, "</COLOR>" ) )
      {
        if( v )
          item.colorid = (int)strtol( v, 0, 10 );
      }
    }
    if( ( s = ccStrFindStrSkip( itemstart, "<CATEGORY>" ) ) )
    {
      char *v = 0;
      if( bsBrickLinkXmlReadString( &v, s, "</CATEGORY>" ) )
      {
        if( v )
          item.categoryid = (int)strtol( v, 0, 10 );
      }
    }
    if( ( s = ccStrFindStrSkip( itemstart, "<QTY>" ) ) )
    {
      char *v = 0;
      if( bsBrickLinkXmlReadString( &v, s, "</QTY>" ) )
      {
        if( v )
          item.quantity = (int)strtol( v, 0, 10 );
      }
    }
    if( ( s = ccStrFindStrSkip( itemstart, "<PRICE>" ) ) )
    {
      char *v = 0;
      if( bsBrickLinkXmlReadString( &v, s, "</PRICE>" ) )
      {
        if( v )
          item.price = (float)strtod( v, 0 );
      }
    }
    if( ( s = ccStrFindStrSkip( itemstart, "<BULK>" ) ) )
    {
      char *v = 0;
      if( bsBrickLinkXmlReadString( &v, s, "</BULK>" ) )
      {
        if( v )
          item.bulk = (int)strtol( v, 0, 10 );
      }
    }
    if( ( s = ccStrFindStrSkip( itemstart, "<LOTID>" ) ) )
    {
      char *v = 0;
      if( bsBrickLinkXmlReadString( &v, s, "</LOTID>" ) )
      {
        if( v )
          item.lotid = (int64_t)strtoll( v, 0, 10 );
      }
    }
    if( ( s = ccStrFindStrSkip( itemstart, "<MYCOST>" ) ) )
    {
      char *v = 0;
      if( bsBrickLinkXmlReadString( &v, s, "</MYCOST>" ) )
      {
        if( v )
          item.mycost = (float)strtod( v, 0 );
      }
    }
    if( ( s = ccStrFindStrSkip( itemstart, "<CONDITION>" ) ) )
    {
      char *v = 0;
      if( bsBrickLinkXmlReadString( &v, s, "</CONDITION>" ) )
      {
        if( ( v ) && ( v[0] ) )
          item.condition = v[0];
      }
    }

    /* DESCRIPTION -> comments (decode entities to be closer to BrickStore) */
    decoded = 0;
    if( ( s = ccStrFindStrSkip( itemstart, "<DESCRIPTION>" ) ) )
    {
      char *v = 0;
      if( bsBrickLinkXmlReadString( &v, s, "</DESCRIPTION>" ) )
      {
        if( v )
        {
          decoded = xmlDecodeEscapeString( v, (int)strlen( v ), &decodedlen );
          if( decoded )
            item.comments = decoded;
          else
            item.comments = v;
        }
      }
    }

    /* REMARKS -> remarks (decode entities) */
    {
      char *decoded2 = 0;
      if( ( s = ccStrFindStrSkip( itemstart, "<REMARKS>" ) ) )
      {
        char *v = 0;
        if( bsBrickLinkXmlReadString( &v, s, "</REMARKS>" ) )
        {
          if( v )
          {
            decoded2 = xmlDecodeEscapeString( v, (int)strlen( v ), &decodedlen );
            if( decoded2 )
              item.remarks = decoded2;
            else
              item.remarks = v;
          }
        }
      }

      bsxVerifyItem( &item );
      bsxAddCopyItem( inv, &item );

      if( decoded )
        free( decoded );
      if( decoded2 )
        free( decoded2 );
    }

    p = itemend;
  }

  free( buf );
  return 1;
}

static void bsBrickLinkReplyInventoryWebXml( void *uservalue, int resultcode, httpResponse *response )
{
  bsContext *context;
  bsQueryReply *reply;
  bsxInventory *inv;

  DEBUG_SET_TRACKER();

  reply = uservalue;
  context = reply->context;

  reply->result = resultcode;
  if( ( response ) && ( response->httpcode != 200 ) )
  {
    if( response->httpcode )
      reply->result = HTTP_RESULT_CODE_ERROR;
    bsStoreError( context, "BrickLink invExcelFinal HTTP Error", response->header, response->headerlength, response->body, response->bodysize );
  }

  /* Parse inventory right here */
  inv = (bsxInventory *)reply->opaquepointer;
  if( ( reply->result == HTTP_RESULT_SUCCESS ) && ( response ) && ( response->body ) )
  {
    if( !( bsBrickLinkParseStoreInventoryXml( inv, response->body, response->bodysize, &context->output ) ) )
    {
      reply->result = HTTP_RESULT_PARSE_ERROR;
      bsStoreError( context, "BrickLink invExcelFinal Parse Error", response->header, response->headerlength, response->body, response->bodysize );
    }
  }

  mmListDualAddLast( &context->replylist, reply, offsetof(bsQueryReply,list) );
  return;
}

static bsxInventory *bsQueryBrickLinkInventoryBrickStoreFallback( bsContext *context )
{
  bsQueryReply *reply;
  bsxInventory *inv;
  bsTracker tracker;
  char *formbody;
  int bodylen;
  char *querystring;

  DEBUG_SET_TRACKER();

  if( !( context->bricklink.brickstoretoken ) )
    return 0;
  if( !( context->bricklink.webhttpshttp ) || !( context->bricklink.accounthttp ) )
    return 0;

  if( !( bsBrickLinkBrickStoreAuthenticate( context ) ) )
    return 0;

  inv = bsxNewInventory();
  bsTrackerInit( &tracker, context->bricklink.webhttpshttp );

  /* Same POST body BrickStore uses */
  formbody = ccStrAllocPrintf( "itemType=&catID=&colorID=&invNew=&itemYear=&viewType=x&invStock=Y&invStockOnly=&invQty=&invQtyMin=0&invQtyMax=0&invBrikTrak=&invDesc=" );
  bodylen = (int)strlen( formbody );

  ioPrintf( &context->output, IO_MODEBIT_FLUSH, BSMSG_INFO "Fetching BrickLink inventory via BrickStore authenticated web endpoint...\n" );

  for( ; ; )
  {
    querystring = ccStrAllocPrintf(
      "POST /invExcelFinal.asp HTTP/1.1\r\n"
      "Host: %s\r\n"
      "Connection: Keep-Alive\r\n"
      "Content-Type: application/x-www-form-urlencoded\r\n"
      "x-bl-tpa-client-id: %s\r\n"
      "x-bl-session-token: %s\r\n"
      "Content-Length: %d\r\n"
      "\r\n"
      "%s",
      BS_BRICKLINK_WEB_SERVER, BS_BRICKLINK_TPA_CLIENT_ID, context->bricklink.sessiontoken, bodylen, formbody
    );

    reply = bsAllocReply( context, BS_QUERY_TYPE_WEBBRICKLINK, 0, 0, (void *)inv );
    httpAddQuery( context->bricklink.webhttpshttp, querystring, ccStrlen( querystring ), HTTP_QUERY_FLAGS_RETRY, (void *)reply, bsBrickLinkReplyInventoryWebXml );
    free( querystring );

    /* Wait until all queries are processed */
    for( ; ; )
    {
      bsFlushTcpProcessHttp( context );
      if( httpGetQueryQueueCount( context->bricklink.webhttpshttp ) > 0 )
        tcpWait( &context->tcp, 0 );
      else
        break;
    }

    if( bsTrackerProcessGenericReplies( context, &tracker, 1 ) )
      break;
    if( tracker.failureflag )
    {
      bsxFreeInventory( inv );
      free( formbody );
      return 0;
    }
  }

  free( formbody );
  return inv;
}



/* Query BrickLink inventory and orderlist at the moment the inventory was taken, return 0 on failure */
bsxInventory *bsQueryBrickLinkFullState( bsContext *context, bsOrderList *orderlist )
{
  int trycount;
  bsOrderList orderlistcheck;
  bsxInventory *inv;
  time_t synctime;

  DEBUG_SET_TRACKER();

  /* Get past orders in respect to inventory */
  /* Loop over if order list changed while retrieving inventory */

#if BS_INTERNAL_DEBUG
  if( httpGetQueryQueueCount( context->bricklink.http ) > 0 )
    BS_INTERNAL_ERROR_EXIT();
#endif

  for( trycount = 0 ; ; trycount++ )
  {
    /* Fetch the BrickLink Order List */
    if( !( bsQueryBickLinkOrderList( context, orderlist, 0, 0 ) ) )
      goto errorstep0;

    synctime = time( 0 );

    /* Fetch the BrickLink inventory */
    inv = bsQueryBrickLinkInventory( context );
    if( !( inv ) )
      goto errorstep1;

    /* If API inventory is empty (common when store is closed), try BrickStore-style download if configured */
    if( ( context->bricklink.brickstoretoken ) && ( !( inv->itemcount - inv->itemfreecount ) ) )
    {
      bsxInventory *altinv = bsQueryBrickLinkInventoryBrickStoreFallback( context );
      if( altinv )
      {
        if( altinv->itemcount - altinv->itemfreecount )
        {
          bsxFreeInventory( inv );
          inv = altinv;
          ioPrintf( &context->output, 0, BSMSG_INFO "BrickLink inventory loaded via BrickStore fallback.\n" );
        }
        else
          bsxFreeInventory( altinv );
      }
    }


    /* We don't want to return an order list with a "topdate" matching the current timestamp */
    if( difftime( time( 0 ), synctime ) < 2.5 )
      ccSleep( 2000 );

    /* Fetch the BrickLink Order List again */
    if( !( bsQueryBickLinkOrderList( context, &orderlistcheck, 0, 0 ) ) )
      goto errorstep2;

    /* Do order lists match after the wait? If yes, break; */
    if( ( orderlist->topdate == orderlistcheck.topdate ) && ( orderlist->topdatecount == orderlistcheck.topdatecount ) )
      break;

    if( trycount >= 5 )
      goto errorstep2;

    /* An order arrived while we were fetching the inventory, isn't that neat? Yeah, start over. */
    ioPrintf( &context->output, 0, BSMSG_INFO "An order arrived while we were retrieving the inventory.\n" );
    bsxEmptyInventory( inv );
    blFreeOrderList( &orderlistcheck );
  }

  ioPrintf( &context->output, 0, BSMSG_INFO "BrickLink inventory has " IO_CYAN "%d" IO_DEFAULT " items in " IO_CYAN "%d" IO_DEFAULT " lots.\n", inv->partcount, inv->itemcount );
  if( !( inv->itemcount - inv->itemfreecount ) )
    ioPrintf( &context->output, 0, BSMSG_WARNING "Is your BrickLink store closed? The inventory of a closed store appears totally empty from the API.\n" );
  blFreeOrderList( &orderlistcheck );
  return inv;

  errorstep2:
  bsxFreeInventory( inv );
  errorstep1:
  blFreeOrderList( orderlist );
  errorstep0:
  return 0;
}


////


/* Handle the reply from BrickOwl to an inventory query */
/* Parse the JSON and build an inventory by matching context's tracked inventory */
static void bsBrickOwlReplyInventory( void *uservalue, int resultcode, httpResponse *response )
{
  bsContext *context;
  bsQueryReply *reply;
  bsxInventory *inv;

  DEBUG_SET_TRACKER();

  reply = uservalue;
  context = reply->context;

#if 0
  ccFileStore( "zzz.txt", data, datasize, 0 );
#endif

  reply->result = resultcode;
  if( ( response ) && ( response->httpcode != 200 ) )
  {
    if( response->httpcode )
      reply->result = HTTP_RESULT_CODE_ERROR;
    bsStoreError( context, "BrickOwl HTTP Error", response->header, response->headerlength, response->body, response->bodysize );
  }
  mmListDualAddLast( &context->replylist, reply, offsetof(bsQueryReply,list) );

  /* Parse inventory right here */
  inv = (bsxInventory *)reply->opaquepointer;
  if( ( reply->result == HTTP_RESULT_SUCCESS ) && ( response->body ) )
  {
    if( !( boReadInventoryTranslate( inv, context->inventory, &context->translationtable, (char *)response->body, &context->output ) ) )
    {
      reply->result = HTTP_RESULT_PARSE_ERROR;
      bsStoreError( context, "BrickOwl JSON Parse Error", response->header, response->headerlength, response->body, response->bodysize );
    }
  }

  return;
}


/* Query the inventory from BrickOwl */
bsxInventory *bsQueryBrickOwlInventory( bsContext *context )
{
  bsQueryReply *reply;
  bsxInventory *inv;
  char *querystring;
  bsTracker tracker;

  DEBUG_SET_TRACKER();

  bsTrackerInit( &tracker, context->brickowl.http );
  inv = bsxNewInventory();
  for( ; ; )
  {
    ioPrintf( &context->output, IO_MODEBIT_FLUSH, BSMSG_INFO "Fetching the BrickOwl Inventory...\n" );
    /* Add an Inventory query */
    querystring = ccStrAllocPrintf( "GET /v1/inventory/list?key=%s%s HTTP/1.1\r\nHost: api.brickowl.com\r\nConnection: Keep-Alive\r\n\r\n", context->brickowl.key, ( context->brickowl.reuseemptyflag ? "&active_only=0" : "" ) );
    reply = bsAllocReply( context, BS_QUERY_TYPE_BRICKOWL, 0, 0, (void *)inv );
    bsBrickOwlAddQuery( context, querystring, HTTP_QUERY_FLAGS_RETRY, (void *)reply, bsBrickOwlReplyInventory );
    free( querystring );
    /* Wait until all queries are processed */
    bsWaitBrickOwlQueries( context, 0 );
    /* Free all queued replies, break on success */
    if( bsTrackerProcessGenericReplies( context, &tracker, 1 ) )
      break;
    if( tracker.failureflag )
    {
      bsxFreeInventory( inv );
      return 0;
    }
  }

  return inv;
}


/* Query BrickOwl diff inventory and orderlist at the moment the inventory was taken, return 0 on failure */
bsxInventory *bsQueryBrickOwlFullState( bsContext *context, bsOrderList *orderlist, int64_t minimumorderdate )
{
  int trycount;
  bsOrderList orderlistcheck;
  time_t synctime;
  bsxInventory *inv;

  DEBUG_SET_TRACKER();

  /* Get past orders in respect to inventory */
  /* Loop over if order list changed while retrieving inventory */

#if BS_INTERNAL_DEBUG
  if( httpGetQueryQueueCount( context->brickowl.http ) > 0 )
    BS_INTERNAL_ERROR_EXIT();
#endif

  for( trycount = 0 ; ; trycount++ )
  {
    /* Fetch the BrickOwl Order List */
    if( !( bsQueryBickOwlOrderList( context, orderlist, minimumorderdate, minimumorderdate ) ) )
      goto errorstep0;

    synctime = time( 0 );

    /* Fetch a BrickOwl diffinv */
    inv = bsQueryBrickOwlInventory( context );
    if( !( inv ) )
      goto errorstep1;

    /* We don't want to return an order list with a "topdate" matching the current timestamp */
    if( difftime( time( 0 ), synctime ) < 2.5 )
      ccSleep( 2000 );

    /* Fetch the BrickOwl Order List again */
    if( !( bsQueryBickOwlOrderList( context, &orderlistcheck, minimumorderdate, minimumorderdate ) ) )
      goto errorstep2;

    /* Do order lists match after the wait? If yes, break; */
    if( ( orderlist->topdate == orderlistcheck.topdate ) && ( orderlist->topdatecount == orderlistcheck.topdatecount ) )
      break;

    if( trycount >= 5 )
      goto errorstep2;

    /* An order arrived while we were fetching the inventory, isn't that neat? Yeah, start over. */
    ioPrintf( &context->output, 0, BSMSG_INFO "An order arrived while we were retrieving the inventory.\n" );
    bsxFreeInventory( inv );
    boFreeOrderList( &orderlistcheck );
  }

  ioPrintf( &context->output, 0, BSMSG_INFO "BrickOwl inventory has " IO_CYAN "%d" IO_DEFAULT " items in " IO_CYAN "%d" IO_DEFAULT " lots.\n", inv->partcount, inv->itemcount );
  boFreeOrderList( &orderlistcheck );
  return inv;

  errorstep2:
  bsxFreeInventory( inv );
  errorstep1:
  boFreeOrderList( orderlist );
  errorstep0:
  return 0;
}


////



