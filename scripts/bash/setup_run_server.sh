# activate the conda pack environment
source /env/bin/activate

usage() {
    echo "Usage: $0 [-g git_repo_url_with_run_server.sh] [-d path_to_dir_with_run_server.sh]" 1>&2
    exit 1
}

while getopts ":g:d:" o; do
    case "${o}" in
    g)
        git_repo_url=${OPTARG}
        ;;
    d)
        dir_path=${OPTARG}
        ;;
    *)
        usage
        ;;
    esac
done
shift $((OPTIND - 1))

if [ -z "${git_repo_url}" ] && [ -z "${dir_path}" ]; then
    usage
fi
if [ -n "${git_repo_url}" ]; then
    echo "git_repo = ${git_repo_url}"
    # git clone the repo which has the run_server.sh
    git clone ${git_repo_url} dash_app
    cd dash_app
fi
if [ -n "${dir_path}" ]; then
    echo "dir_path = ${dir_path}"
    cd ${dir_path}
fi
# TODO: check that run_server.sh exists in this path else fail

# run_server.sh in the git repo should start a web server listening to port 80
# e.g. with holoviz panel like
# panel serve my_dash.ipynb --address 0.0.0.0 --port 80 --allow-websocket-origin="*"
source run_server.sh
