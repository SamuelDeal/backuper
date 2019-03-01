def map_report(report, params):

    result = params
    result["username"] = "Backups"
    if report.is_success:
        result["text"] = "Backup: success"
    else:
        result["text"] = "Backup: " + str(report.issue_count) + " errors"

    result["attachments"] = []
    for server, issues in report.all_issues.items():
        if not issues:
            continue
        result["attachments"].append({
            "title": "server: "+server+": "+str(len(issues))+" errors",
            "title_link": server,
            "text": "\n".join(issues),
            "color": "danger"
        })
    for server, warnings in report.all_warnings.items():
        if not warnings:
            continue
        result["attachments"].append({
            "title": "server: " + server + ": " + str(len(warnings)) + " warnings",
            "title_link": server,
            "text": "\n".join(warnings),
            "color": "warning"
        })
    for server, success in report.all_success.items():
        if not success:
            continue
        result["attachments"].append({
            "title": "server: " + server + ": ",
            "title_link": server,
            "text": "\n".join(success),
            "color": "good"
        })
    return result
