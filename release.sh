#!/bin/bash

increment_version() {
    while getopts ":Mmp" Option
    do
      case ${Option} in
        M ) major=true;;
        m ) minor=true;;
        p ) patch=true;;
      esac
    done

    shift $(($OPTIND - 1))

    version=$1

    # Build array from version string.

    a=( ${version//./ } )

    # If version string is missing or has the wrong number of members, show usage message.

    if [[ ${#a[@]} -ne 3 ]]
    then
      echo "usage: $(basename $0) [-Mmp] major.minor.patch"
      exit 1
    fi

    # Increment version numbers as requested.

    if [[ -n ${major} ]]
    then
      ((a[0]++))
      a[1]=0
      a[2]=0
    fi

    if [[ -n $minor ]]
    then
      ((a[1]++))
      a[2]=0
    fi

    if [[ -n $patch ]]
    then
      ((a[2]++))
    fi

    echo "${a[0]}.${a[1]}.${a[2]}"
}

# Ensure that the working repo is clean
if [[ -n "$(git status --untracked-files=no --porcelain)" ]]; then
    echo -e "\n**** WARNING ****"
    echo -e "You're local git repo has staged, uncommitted changes.\n"
    while true; do
        read -e -p "If you continue, changes will be stashed prior to performing the release, continue? [y/n] "  confirm_stash
        case ${confirm_stash} in
            [yY][eE][sS]|[yY])
                git stash
                break
                ;;
            [nN][oO]|[nN])
                echo "Exiting."
                exit 0
                ;;
            *)
                echo "Invalid input, try again"
                ;;
        esac
    done
fi

echo -e "   Getting current version information..."
CURRENT_BRANCH=`git branch | grep \* | cut -d ' ' -f2`
CURRENT_VERSION=`mvn -Dexec.executable='echo' -Dexec.args='${project.version}' --non-recursive exec:exec -q`
echo -e "   found version ${CURRENT_VERSION} on branch ${CURRENT_BRANCH}"

PROPOSED_VERSION=`increment_version -m ${CURRENT_VERSION}`
read -e -p "   Enter new release version [${PROPOSED_VERSION}]? " VERSION
if [[ -z ${VERSION} ]]; then
    VERSION=${PROPOSED_VERSION}
fi

PROPOSED_SNAPSHOT=`increment_version -m ${PROPOSED_VERSION}`-SNAPSHOT
read -e -p "   Enter new snapshot version [${PROPOSED_SNAPSHOT}]? " SNAPSHOT_VERSION
if [[ -z ${SNAPSHOT_VERSION} ]]; then
    SNAPSHOT_VERSION=${PROPOSED_SNAPSHOT}
fi

while true; do
    read -e -p "   Promoting to version ${VERSION}, bumping to snapshot ${SNAPSHOT_VERSION}, continue? [Y/n] "  CONFIRM_VERSION
    case ${CONFIRM_VERSION} in
        [yY][eE][sS]|[yY])
            break
            ;;
        [nN][oO]|[nN])
            echo -e "   Declined. Exiting. "
            exit 0
            ;;
        *)
            echo -e "   \nYou're answer was '${CONFIRM_VERSION}'. Yes or no is required, try again."
            ;;
    esac
done

git checkout master

# Perform a git flow release
git-flow release start ${VERSION}
mvn versions:set -DnewVersion=${VERSION}
git commit -a -m "Promote to release ${VERSION}"
git-flow release finish -m "finish" ${VERSION}

# Push release to master
git push origin master

# Push newly created tag to origin
git push origin rel-${VERSION}

# Update develop with new version number
git checkout develop
mvn versions:set -DnewVersion=${SNAPSHOT_VERSION}
git add *pom.xml "**/*pom.xml"
git commit -m "Bumped to ${SNAPSHOT_VERSION}"

# Push new SNAPSHOT to origin develop
git push origin develop
