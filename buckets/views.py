from django.shortcuts import render
from boto3 import client as boto3_client
from os.path import join as path_join
from django.http import HttpResponse, Http404
from django.conf import settings
from botocore.exceptions import ClientError
import mimetypes

s3_client = boto3_client(
    "s3",
    aws_access_key_id="#",
    aws_secret_access_key="#",
)

def download_file(request):
    """
    Downloads a file from S3.
    """ 
    key = request.GET.get("key")
    bucket = request.GET.get("bucket")

    try:
        # Attempt to download the file
        filename = key.split('/')[-1]
        save_path = f"{settings.MEDIA_ROOT.rstrip('/')}/{filename}"
        object = s3_client.get_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.NoSuchKey:
        raise Http404()

    mime_type, _ = mimetypes.guess_type(save_path)
    response = HttpResponse(object["Body"].read(), content_type=mime_type)
    response['Content-Disposition'] = "attachment; filename=%s" % filename
    return response


def list_objects(request, path):
    """
    Lists objects in a given S3 bucket or prefix.
    """
    current_path = request.get_full_path()

    # Parse out bucket and prefix from the path
    split_paths = path.split("/")
    bucket = split_paths[0]
    prefix = "/".join(split_paths[1:]).lstrip("/") + "/"

    # Perform the S3 list
    paginator = s3_client.get_paginator("list_objects_v2")
    # page_iterator = paginator.paginate(Bucket=bucket, PaginationConfig={'StartingToken': ''})
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    outputs = []
    next_token = None

    for page in page_iterator:
        next_token = page.get("NextToken")
        if "Contents" in page:
            contents = page.get("Contents")
            for content in contents:
                key = content.get("Key").replace(prefix, "")
                key_is_folder = key.endswith("/")
                if len(list(filter(lambda p: p != "", key.split("/")))) == 1:
                    outputs.append({
                        "key": key,
                        "href": path_join(current_path, key) if key_is_folder else f"/object/download?bucket={bucket}&key={content.get('Key')}",
                        "type": "Folder" if key_is_folder else key.split(".")[-1],
                        "size": "-" if key_is_folder else content["Size"],
                        "last_modified": "-" if key_is_folder else content["LastModified"],
                    })

    context = {"contents": outputs, "next_token": next_token}

    return render(request, "buckets/templates/list_objects.html", context)
