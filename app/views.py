from django.shortcuts import render
from django.conf import settings
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

import cloudlibs
import json

# Create your views here.
def home(request):
  return render(request, "index.html")

def _make_proxy(setting):

    return cloudlibs.proxy(setting['host'], setting['app_id'], setting['pipe_id'], setting['pipe_secret'])

def _get_http_response(msg, type="success", mimetype="application/json"):

    status = 200

    if type == "error":
        status = 500

    http_response = HttpResponse(msg, content_type=mimetype, status=status)

    return http_response

@csrf_exempt
def search(request):

  search_proxy = _make_proxy(settings.SEARCH)

  filters = json.loads(request.POST.get("filters", "{}"))
  size = request.POST.get("size", 20)

  query = "*"

  if filters:

    queryVals = []

    for key, data in filters.items():

      if data["type"] == "regex":
        queryVals.append("%s:/.*%s.*/" % (key, data["value"]))
      elif data["type"] == "search":
        queryVals.append("%s:\"%s\"" % (key, data["value"]))
      elif data["type"] == "range":
        queryVals.append("%s:%s" % (key, data["value"]))

    query = " AND ".join(queryVals)

  print query

  query = {"query":{"query":{"query_string":{"query":query, "use_dis_max":True}}, "sort":[{"dt_added":{"order":"desc"}}], "size":size},"indexes":["realestate"],"doc_types":"item","query_params":{"scroll":"120m"}}

  resp = search_proxy.search(**query)

  return _get_http_response(json.dumps(resp))

@csrf_exempt
def scroll(request):

  search_proxy = _make_proxy(settings.SEARCH)

  scroll_id = request.POST.get("scrollID")

  query = {"scroll_id": scroll_id, "scroll_timeout": "120m"}

  resp = search_proxy.search_scroll(**query)

  return _get_http_response(json.dumps(resp))
