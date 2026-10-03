#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

FALLBACK_REPO = "RJ-SMTR/pipelines_v3"
DEFAULT_PROJECT_OWNER = "RJ-SMTR"
DEFAULT_PROJECT_NUMBER = 21
COMMAND_TIMEOUT_SECONDS = 60
DEFAULT_FIELDS = {
    "status": "To Do",
    "apetite": "⏱️ 2 semanas",
    "raia": "🗂️ Small Batch",
}
FALLBACK_NUCLEO = "Inovação"


def run_command(args, input_text=None):
    try:
        return subprocess.run(
            args,
            input=input_text,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        command = " ".join(args)
        raise RuntimeError(
            f"command timed out after {COMMAND_TIMEOUT_SECONDS}s: {command}"
        ) from exc


def run_gh(args, input_text=None):
    proc = run_command(["gh", *args], input_text=input_text)
    if proc.returncode != 0:
        raise RuntimeError(
            "gh command failed:\n"
            f"  gh {' '.join(args)}\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )
    return proc.stdout


def graphql(query, variables):
    payload = json.dumps({"query": query, "variables": variables})
    return json.loads(run_gh(["api", "graphql", "--input", "-"], input_text=payload))


def current_repo():
    try:
        output = run_gh(["repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner"])
        repo = output.strip()
        if repo:
            return repo
    except Exception:
        pass

    proc = run_command(["git", "remote", "get-url", "origin"])
    if proc.returncode == 0:
        remote = proc.stdout.strip()
        match = re.search(r"github\.com[:/](?P<repo>[^/]+/[^/.]+)(?:\.git)?$", remote)
        if match:
            return match.group("repo")
    return FALLBACK_REPO


def repo_owner_name(repo):
    if "/" not in repo:
        raise ValueError("repo must be OWNER/NAME")
    return repo.split("/", 1)


def get_context(repo, parent_number, project_owner, project_number):
    owner, name = repo_owner_name(repo)
    query = """
    query($owner:String!, $name:String!, $number:Int!, $projectOwner:String!, $projectNumber:Int!) {
      viewer {
        id
        login
      }
      repository(owner:$owner, name:$name) {
        id
        issueTypes(first:20) { nodes { id name isEnabled } }
        issue(number:$number) {
          id
          number
          title
          url
          assignees(first:20) { nodes { id login } }
          subIssues(first:50) { nodes { number title url } }
          projectItems(first:20) {
            nodes {
              project { id number title }
              fieldValues(first:50) {
                nodes {
                  ... on ProjectV2ItemFieldSingleSelectValue {
                    name
                    field { ... on ProjectV2FieldCommon { name } }
                  }
                }
              }
            }
          }
        }
      }
      organization(login:$projectOwner) {
        projectV2(number:$projectNumber) {
          id
          number
          title
          fields(first:50) {
            nodes {
              ... on ProjectV2Field { id name dataType }
              ... on ProjectV2SingleSelectField { id name dataType options { id name } }
            }
          }
        }
      }
    }
    """
    data = graphql(
        query,
        {
            "owner": owner,
            "name": name,
            "number": parent_number,
            "projectOwner": project_owner,
            "projectNumber": project_number,
        },
    )["data"]
    repository = data.get("repository")
    if repository is None:
        raise ValueError(f"repository not found: {owner}/{name}")
    if repository.get("issue") is None:
        raise ValueError(f"parent issue #{parent_number} not found in {owner}/{name}")
    organization = data.get("organization")
    project = organization.get("projectV2") if organization else None
    if project is None:
        raise ValueError(f"Project {project_owner}/{project_number} not found")
    return repository, project, data["viewer"]


def escopo_issue_type_id(repository):
    for issue_type in repository.get("issueTypes", {}).get("nodes", []):
        if issue_type.get("isEnabled") and issue_type.get("name") == "🔨 Escopo":
            return issue_type["id"]
    return None


def parent_nucleo(parent_issue, project_id):
    for item in parent_issue.get("projectItems", {}).get("nodes", []):
        if item.get("project", {}).get("id") != project_id:
            continue
        for field_value in item.get("fieldValues", {}).get("nodes", []):
            field = field_value.get("field") or {}
            if field.get("name") == "Núcleo":
                return field_value.get("name")
    return None


def create_issue(  # noqa: PLR0913
    repository_id, title, body, assignee_ids, project_id, parent_id, issue_type_id
):
    query = """
    mutation(
      $repositoryId:ID!, $title:String!, $body:String!, $assigneeIds:[ID!],
      $projectId:ID!, $parentId:ID!, $issueTypeId:ID
    ) {
      createIssue(input:{
        repositoryId:$repositoryId,
        title:$title,
        body:$body,
        assigneeIds:$assigneeIds,
        projectV2Ids:[$projectId],
        parentIssueId:$parentId,
        issueTemplate:"escopo",
        issueTypeId:$issueTypeId
      }) {
        issue { id number title url issueType { name } }
      }
    }
    """
    data = graphql(
        query,
        {
            "repositoryId": repository_id,
            "title": title,
            "body": body,
            "assigneeIds": assignee_ids,
            "projectId": project_id,
            "parentId": parent_id,
            "issueTypeId": issue_type_id,
        },
    )
    return data["data"]["createIssue"]["issue"]


def add_to_project(project_id, content_id):
    query = """
    mutation($projectId:ID!, $contentId:ID!) {
      addProjectV2ItemById(input:{projectId:$projectId, contentId:$contentId}) {
        item { id }
      }
    }
    """
    data = graphql(query, {"projectId": project_id, "contentId": content_id})
    return data["data"]["addProjectV2ItemById"]["item"]["id"]


def find_project_item(project_id, issue_id):
    query = """
    query($issueId:ID!) {
      node(id:$issueId) {
        ... on Issue {
          projectItems(first:20) {
            nodes { id project { id } }
          }
        }
      }
    }
    """
    data = graphql(query, {"issueId": issue_id})["data"]["node"]["projectItems"]["nodes"]
    for item in data:
        if item["project"]["id"] == project_id:
            return item["id"]
    return None


def field_maps(project):
    fields = {}
    options = {}
    for field in project["fields"]["nodes"]:
        if not field:
            continue
        fields[field["name"]] = field
        if field.get("options"):
            options[field["name"]] = {option["name"]: option["id"] for option in field["options"]}
    return fields, options


def normalized_option_name(value):
    return re.sub(r"^[^\w]+", "", value, flags=re.UNICODE).casefold()


def resolve_option(option_map, requested_value, field_name):
    if requested_value in option_map:
        return requested_value, option_map[requested_value]

    normalized = normalized_option_name(requested_value)
    matches = [
        (name, option_id)
        for name, option_id in option_map.items()
        if normalized_option_name(name) == normalized
    ]
    if len(matches) == 1:
        return matches[0]

    valid = ", ".join(option_map)
    if len(matches) > 1:
        raise ValueError(f"Ambiguous option for {field_name}: {requested_value}. Valid: {valid}")
    raise ValueError(f"Invalid option for {field_name}: {requested_value}. Valid: {valid}")


def set_single_select(project_id, item_id, field_id, option_id):
    query = """
    mutation($projectId:ID!, $itemId:ID!, $fieldId:ID!, $optionId:String!) {
      updateProjectV2ItemFieldValue(input:{
        projectId:$projectId,
        itemId:$itemId,
        fieldId:$fieldId,
        value:{singleSelectOptionId:$optionId}
      }) {
        projectV2Item { id }
      }
    }
    """
    graphql(
        query,
        {
            "projectId": project_id,
            "itemId": item_id,
            "fieldId": field_id,
            "optionId": option_id,
        },
    )


def resolve_assignees(parent_issue, issue_spec, viewer, mode):
    parent_assignees = {node["login"]: node["id"] for node in parent_issue["assignees"]["nodes"]}
    requested = issue_spec.get("assignees")
    if requested is None:
        if mode == "blank":
            return []
        if mode == "viewer":
            return [viewer["id"]]
        if mode == "parent":
            return list(parent_assignees.values())
        if mode == "parent-fallback-viewer":
            parent_ids = list(parent_assignees.values())
            return parent_ids or [viewer["id"]]
        raise ValueError(f"unsupported assignee mode: {mode}")
    missing = [login for login in requested if login not in parent_assignees]
    if viewer["login"] in missing:
        missing.remove(viewer["login"])
    if missing:
        raise ValueError(
            "assignees must be assigned on the parent issue or match the authenticated user: "
            + ", ".join(missing)
        )
    ids = []
    for login in requested:
        if login in parent_assignees:
            ids.append(parent_assignees[login])
        elif login == viewer["login"]:
            ids.append(viewer["id"])
    return ids


def apply_fields(project, item_id, issue_spec, inherited_nucleo, dry_run):
    fields, options = field_maps(project)
    values = dict(DEFAULT_FIELDS)
    values.update({k: v for k, v in issue_spec.items() if k in values and v})
    values["nucleo"] = issue_spec.get("nucleo") or inherited_nucleo or FALLBACK_NUCLEO
    if issue_spec.get("progresso"):
        values["progresso"] = issue_spec["progresso"]

    field_name_by_key = {
        "status": "Status",
        "apetite": "Apetite",
        "raia": "Raia",
        "nucleo": "Núcleo",
        "progresso": "Progresso",
    }

    applied = {}
    for key, value in values.items():
        field_name = field_name_by_key[key]
        if field_name not in fields:
            raise ValueError(f"Project field not found: {field_name}")
        canonical_value, option_id = resolve_option(options.get(field_name, {}), value, field_name)
        applied[field_name] = canonical_value
        if not dry_run:
            set_single_select(project["id"], item_id, fields[field_name]["id"], option_id)
    return applied


def load_plan(path):
    plan = json.loads(Path(path).read_text())
    issues = plan.get("issues")
    if not isinstance(issues, list) or not issues:
        raise ValueError("plan must contain a non-empty issues list")
    for index, issue in enumerate(issues, start=1):
        if not issue.get("title") or not issue.get("body"):
            raise ValueError(f"issue #{index} must contain title and body")
    return issues


def print_created_summary(created, heading="\nCreated issues:"):
    if not created:
        return
    print(heading)
    for item in created:
        issue = item["issue"]
        suffix = "" if item["fields"] is not None else " (Project fields incomplete)"
        print(f"- #{issue['number']} {issue['url']}{suffix}")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent", type=int, required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--repo", default=None)
    parser.add_argument("--project-owner", default=DEFAULT_PROJECT_OWNER)
    parser.add_argument("--project-number", type=int, default=DEFAULT_PROJECT_NUMBER)
    parser.add_argument(
        "--assignee-mode",
        choices=["parent-fallback-viewer", "parent", "viewer", "blank"],
        default="parent-fallback-viewer",
        help="Default assignee behavior when an issue spec does not set assignees.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()

    repo = args.repo or current_repo()
    issues = load_plan(args.plan)
    repository, project, viewer = get_context(
        repo, args.parent, args.project_owner, args.project_number
    )
    parent_issue = repository["issue"]
    issue_type_id = escopo_issue_type_id(repository)
    inherited_nucleo = parent_nucleo(parent_issue, project["id"])
    existing_titles = {node["title"].casefold() for node in parent_issue["subIssues"]["nodes"]}

    print(f"Repo: {repo}")
    print(f"Parent: #{parent_issue['number']} {parent_issue['title']}")
    print(f"Project: {project['title']} #{project['number']}")
    print(f"Issue type: {'🔨 Escopo' if issue_type_id else 'not found'}")
    print(f"Parent Núcleo: {inherited_nucleo or f'not set; fallback {FALLBACK_NUCLEO}'}")
    print(f"Assignee mode: {args.assignee_mode} (viewer: {viewer['login']})")
    print(f"Mode: {'dry-run' if args.dry_run else 'create'}")

    created = []
    for issue_spec in issues:
        title = issue_spec["title"].strip()
        if title.casefold() in existing_titles:
            print(f"SKIP duplicate title: {title}")
            continue
        existing_titles.add(title.casefold())

        assignee_ids = resolve_assignees(parent_issue, issue_spec, viewer, args.assignee_mode)
        if args.dry_run:
            applied = apply_fields(
                project, "DRY_RUN_ITEM", issue_spec, inherited_nucleo, dry_run=True
            )
            print(f"WOULD CREATE: {title}")
            print(f"  assignee ids: {assignee_ids or 'none'}")
            print(f"  fields: {applied}")
            continue

        issue = create_issue(
            repository["id"],
            title,
            issue_spec["body"],
            assignee_ids,
            project["id"],
            parent_issue["id"],
            issue_type_id,
        )
        created_item = {"issue": issue, "fields": None}
        created.append(created_item)
        try:
            item_id = find_project_item(project["id"], issue["id"])
            if item_id is None:
                item_id = add_to_project(project["id"], issue["id"])
            applied = apply_fields(project, item_id, issue_spec, inherited_nucleo, dry_run=False)
            created_item["fields"] = applied
        except Exception:
            print_created_summary(created, "\nCreated issues before failure:")
            raise
        print(f"CREATED: #{issue['number']} {issue['title']}")
        print(f"  {issue['url']}")
        print(f"  fields: {applied}")

    print_created_summary(created)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
