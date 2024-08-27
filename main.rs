//author Yesho Reddipalli

#![allow(non_snake_case)]
use chrono::{DateTime, NaiveDateTime, Utc};
use filetime::FileTime;
use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::{self};
use std::path::{PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime};
use walkdir::{IntoIter, WalkDir};

fn main() {
    let target_directory = get_target_directory();
    let walk_dir = WalkDir::new(target_directory.clone());
    let depth_map = sort_by_depth(&mut walk_dir.into_iter());
    let dir = PathBuf::from(target_directory.clone());
    operate_by_grand_parent(depth_map);
    copy_files_to_unified_dir(&dir);
}

// Used to get the target directory

fn get_target_directory() -> String {
    let output = Command::new("whoami")
        .stdout(Stdio::piped())
        .output()
        .unwrap();
    let current_user = String::from_utf8(output.stdout).unwrap().trim().to_string();
    format!("/home/{}/Documents/project/data", current_user)
}

/// produces a HashMap of files and directories organized by their given depth
///
/// # Arguments
///
/// * `walk`: An IntoIter from a WalkDir that has all the files and directories located in the selected directory
///
/// returns: HashMap<usize, Vec<PathBuf, Global>, RandomState>
/// the key in the HashMap represents the given depth that all files and directories are located at
/// the value in the HashMap is an unordered vector of all the given files and  directories at the given depth
///
fn sort_by_depth(walk: &mut IntoIter) -> HashMap<usize, Vec<PathBuf>> {
    let mut files_by_depth: HashMap<usize, Vec<PathBuf>> = HashMap::with_capacity(15);
    while let Some(Ok(entry)) = walk.next() {
        let depth = entry.depth();
        files_by_depth
            .entry(depth)
            .or_insert_with(Vec::new)
            .push(entry.into_path());
    }
    files_by_depth
}

/// takes files organized by depth reorganizes them by their grandparent files and performs some
/// a determined operation on them
///
/// # Arguments
///
/// * `depth_map`: a HashMap of files and directories organized by their their depth
/// * `process`: the operation to be performed on the files organized by grandparents
/// returns: ()
///
fn operate_by_grand_parent(depth_map: HashMap<usize, Vec<PathBuf>>) {
    depth_map.par_iter().for_each(|(_, files)| {
        let files = files.into_iter().filter(|entry| entry.is_file());
        let mut temp_branch: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for file in files {
            if let Some(leaf_files) = temp_branch.get_mut(file.parent().unwrap()) {
                leaf_files.push(file.to_path_buf())
            } else {
                let parent_name = file.parent().unwrap().to_path_buf();
                temp_branch.insert(parent_name, vec![file.to_path_buf()]);
            }
        }
        let branch: HashMap<PathBuf, Vec<Vec<PathBuf>>> =
            temp_branch
                .into_iter()
                .fold(HashMap::new(), |mut acc, (parent, files)| {
                    let grand_parent = parent.parent().unwrap().to_path_buf();
                    acc.entry(grand_parent)
                        .or_insert_with(|| Vec::new())
                        .push(files);
                    acc
                });
        match_most_common_and_refactor(branch);
    });
}

/// takes a map of directories organized by grandparent directory, matches the most closely related files,
/// and and refactors the files names and timestamps
///
/// # Arguments
///
/// * `grand_parents_map`: a HashMap consisting of keys that represent grandparent directories and
/// values of vector containing vectors paired by matching parent files
///
/// returns: ()
///
fn match_most_common_and_refactor(grand_parents_map: HashMap<PathBuf, Vec<Vec<PathBuf>>>) {

    grand_parents_map.into_par_iter().for_each(|(_, mut dir)| {
        while let Some(mut dirs_by_relation) = relate_dir(&dir) {
            let (lead_dir, paired_dir) = dirs_by_relation.pop().unwrap();
            if paired_dir.is_empty() {
                if let Some(mut files) = dir.pop() {
                    if !files.is_empty() {
                        files.shuffle(&mut thread_rng());
                        files
                            .into_par_iter()
                            .enumerate()
                            .for_each(|(i, file)| refactor_file(file, i));
                    }
                }
                break;
            }
            let (dir_1, dir_2) = remove_paired_dir(lead_dir, paired_dir, &mut dir);
            let dir_1 = temp_file_names(dir_1);
            let dir_2 = temp_file_names(dir_2);
            let mut pair_files = match_paired_files(&dir_1, &dir_2);
            let (mut unpaired_1, mut unpaired_2) = match_unpaired_files(dir_1, dir_2);
            while !(unpaired_1.is_empty() || unpaired_2.is_empty()) {
                pair_files.push((unpaired_1.pop(), unpaired_2.pop()));
            }
            pair_files.shuffle(&mut thread_rng());
            unpaired_1.shuffle(&mut thread_rng());
            unpaired_2.shuffle(&mut thread_rng());
            while !(unpaired_1.is_empty() && unpaired_2.is_empty()) {
                pair_files.push((unpaired_1.pop(), unpaired_2.pop()));
            }
            refactor_pairs(pair_files);
        }
    });
}

/// determines how closely related all directories that share the same grandparent file are to each other
/// returning a vector of tuples of a directory and the other directories with the total number of matching files
///
/// # Arguments
///
/// * `directories`: takes a vector of vectors representing a directory and the files contained in the directory
///
/// returns: Option<Vec<(PathBuf, Vec<(PathBuf, i32), Global>), Global>>
/// a vector that shows how closely related each directory is to all other directories
///
fn relate_dir(directories: &Vec<Vec<PathBuf>>) -> Option<Vec<(PathBuf, Vec<(PathBuf, usize)>)>> {
    if directories.is_empty() {
        return None;
    }
    if directories.len() == 1 {
        let parent_dir = directories[0][0].parent()?.to_path_buf();
        return Some(vec![(parent_dir, vec![])]);
    }
    let mut relations = vec![];
    for outer in directories.iter() {
        let mut comparison = Vec::new();
        for inner in directories.iter().skip_while(|&x| x != outer).skip(1) {
            let count = count_matching_files(inner, outer);
            comparison.push((inner[0].parent()?.to_path_buf(), count));
        }
        if !comparison.is_empty() {
            comparison.sort_by(|x, y| y.1.cmp(&x.1));
            relations.push((outer[0].parent()?.to_path_buf(), comparison));
        }
    }
    relations.sort_by(|x, y| x.1[0].1.cmp(&y.1[0].1));
    Some(relations)
}

/// returns the count of files that have the same name in both directories.
///
/// # Arguments
///
/// * `dir_1`: first directory
/// * `dir_2`: second directory
///
/// returns: i32
///
fn count_matching_files(dir_1: &Vec<PathBuf>, dir_2: &Vec<PathBuf>) -> usize {
    let dir_1: HashSet<_> = dir_1.iter().map(|f| f.file_name()).collect();
    let dir_2: HashSet<_> = dir_2.iter().map(|f| f.file_name()).collect();
    dir_2.iter().filter(|&f| dir_1.contains(f)).count()
}

/// returns a vector of tuples, where each tuple contains two Option<PathBuf> objects.
/// The tuples represent pairs of files that have the same name in the two directories.
///
/// # Arguments
///
/// * `lead_dir`: the original directory
/// * `paired_dir`: the directory most closely related to the original directory
/// * `dir`: the total collection of directories that the directories will be removed from
///
/// returns: (Vec<PathBuf, Global>, Vec<PathBuf, Global>)
///
fn remove_paired_dir(
    lead_dir: PathBuf,
    paired_dir: Vec<(PathBuf, usize)>,
    dir: &mut Vec<Vec<PathBuf>>,
) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let (i, _) = dir
        .iter()
        .find_position(|dirs| dirs[0].parent().unwrap() == lead_dir)
        .unwrap();
    let match_1 = dir.remove(i);
    let (j, _) = dir
        .iter()
        .find_position(|dirs| dirs[0].parent().unwrap() == paired_dir[0].0)
        .unwrap();
    let match_2 = dir.remove(j);
    (match_1, match_2)
}

/// files that share the same name but differing parent directories together
///
/// # Arguments
///
/// * `dir_1`: first directory
/// * `dir_2`: second directory
///
/// returns: Vec<(Option<PathBuf>, Option<PathBuf>), Global>
///
fn match_paired_files(
    dir_1: &Vec<PathBuf>,
    dir_2: &Vec<PathBuf>,
) -> Vec<(Option<PathBuf>, Option<PathBuf>)> {
    let mut pair = vec![];
    for file_1 in dir_1 {
        let file_1_name = file_1.file_name().unwrap();
        if let Some(file_2) = dir_2
            .iter()
            .find(|file_2| file_2.file_name().unwrap() == file_1_name)
        {
            pair.push((Some(file_1.clone()), Some(file_2.clone())))
        }
    }
    pair
}

/// determines all files in two different directories that do not match
/// then collects them into a vector of tuples and returns them
///
/// # Arguments
///
/// * `dir_1`: first directory
/// * `dir_2`: second directory
///
/// returns: (Vec<PathBuf, Global>, Vec<PathBuf, Global>)
///
fn match_unpaired_files(dir_1: Vec<PathBuf>, dir_2: Vec<PathBuf>) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let set_1: HashSet<_> = dir_1.iter().map(|p| p.file_name().unwrap()).collect();
    let set_2: HashSet<_> = dir_2.iter().map(|p| p.file_name().unwrap()).collect();
    let unpaired_1 = dir_1
        .clone()
        .into_iter()
        .filter(|p| !set_2.contains(p.file_name().unwrap()))
        .collect();
    let unpaired_2 = dir_2
        .into_iter()
        .filter(|p| !set_1.contains(p.file_name().unwrap()))
        .collect();
    (unpaired_1, unpaired_2)
}

/// renames a file to an integer value and outputs the files old name and creation date
/// then the files new name and a random creation date no more then ten days before its creation
///
/// # Arguments
///
/// * `file`: the files to be renamed
/// * `index`: the location in the vector the file is located

fn refactor_file(file: PathBuf, index: usize) {
    let path = file.to_str().unwrap();
    let stem = file.file_stem().unwrap().to_str().unwrap();
    let stem = &stem[..stem.len() - 1];
    let extension = file.extension().unwrap().to_str().unwrap();
    let dir_name = file.parent().unwrap().to_str().unwrap();
    let original_name = format!("{}/{}.{}", dir_name, stem, extension);
    let new_name = format!("{}/{}.{}", dir_name, index + 1, extension);
    let (original_time, new_time) = determine_time(&file);
    fs::rename(path, &new_name).unwrap();
    let new_time = new_time.unwrap();
    let original_time = original_time.unwrap();
    filetime::set_file_times(&new_name, new_time, new_time).unwrap();
    let original_time = convert_time_format(original_time);
    let new_time = convert_time_format(new_time);
    println!("Original file name: {}\nOriginal file time: {:?}\nNew file name: {}\nNew file time: {:?}",
             original_name, original_time, new_name, new_time );
}

fn copy_files_to_unified_dir(source_path: &PathBuf) {
    let destination_path = create_dir();
    for entry in WalkDir::new(&source_path)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
    {
        let file_name = entry
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let mut destination_path = destination_path.clone();
        if entry.path().parent().unwrap().ends_with("left"){
            destination_path.push("left")
        }
        else if  entry.path().parent().unwrap().ends_with("right"){
            destination_path.push("right")
        }
        let full_name  = entry.path().ancestors().nth(2).unwrap().to_str().unwrap().replace("/", "_");
        let new_name = format!("{}_{}",full_name,file_name);
        destination_path.push(PathBuf::from(new_name));
        match fs::rename(entry.path(), &destination_path) {
            Ok(_) => {
                println!("{} moved to directory full_dataset and renamed ", &file_name )
            }
            Err(_e) => {},
        };
    }
}

// Create the full_dataset directory if it doesn't exist
// If it does not exist then create both left and right directories 
fn create_dir()-> PathBuf{
    let output = Command::new("whoami")
        .stdout(Stdio::piped())
        .output()
        .unwrap();
    let current_user = String::from_utf8(output.stdout).unwrap().trim().to_string();
    let mut path = PathBuf::new();
    let target_directory = format!("/home/{}/Documents/project", current_user);
    path.push(target_directory.clone());
    path.push("full_dataset");
    
    if path.exists(){
       match fs::remove_dir_all(&path){
            Ok(_) => {},
            Err(_e) => {},
        }
    }
     if !path.exists() {
        match fs::create_dir_all(&path) {
            Ok(_) => {
                fs::create_dir(format!("{}/full_dataset/left", target_directory.clone())).unwrap();
                fs::create_dir(format!("{}/full_dataset/right", target_directory.clone())).unwrap();
            }
            Err(_e) => {},
        }
    }
    path
}


/// takes a vector of paired files iterates there the vector changing the name and date of all the files
/// if a file is a pair of another file it will have the same name as the matching file
///
/// # Arguments
///
/// * `files`: the vector of matching files
///
/// returns: ()
///
fn refactor_pairs(files: Vec<(Option<PathBuf>, Option<PathBuf>)>) {
    files
        .into_par_iter()
        .enumerate()
        .for_each(|(i, (f_1, f_2))| {
            if let Some(file) = f_1 {
                refactor_file(file, i);
            }
            if let Some(file) = f_2 {
                refactor_file(file, i);
            }
        });
}

/// takes a FileTime object and converts it to a DateTime<Utc> object
/// representing the same date and time in Coordinated Universal Time (UTC).
///
/// # Arguments
///
/// * `time`: the time of the file as FileTime
///
/// returns: DateTime<Utc>
///
fn convert_time_format(time: FileTime) -> DateTime<Utc> {
    let time = NaiveDateTime::from_timestamp_opt(time.unix_seconds(), 0).unwrap();
    DateTime::from_utc(time, Utc)
}

/// takes a file and returns its date of creation and a new randomly generated creation time
///
/// # Arguments
///
/// * `file`: the file to have its time pulled from
///
/// returns: (Option<FileTime>, Option<FileTime>)
///
fn determine_time(file: &PathBuf) -> (Option<FileTime>, Option<FileTime>) {
    const SEC_IN_10_DAYS: i64 = 10 * 24 * 60 * 60;
    const SEC_IN_HOUR: i64 = 60 * 60;
    if let Ok(metadata) = file.metadata() {
        let original_time = FileTime::from_last_modification_time(&metadata);
        let now = SystemTime::now();
        let rand_secs = SEC_IN_HOUR + thread_rng().gen_range(0..=(SEC_IN_10_DAYS - SEC_IN_HOUR));
        let new_time = FileTime::from_system_time(now - Duration::from_secs(rand_secs as u64));
        return (Some(original_time), Some(new_time));
    }
    (None, None)
}


fn temp_file_names(dir: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut renamed_files = vec![];
    for file in dir {
        let original_time = FileTime::from_last_modification_time(&file.metadata().unwrap());
        let name = file.file_stem().unwrap().to_str().unwrap();
        let extension = file.extension().unwrap().to_str().unwrap();
        let dir_name = file.parent().unwrap().to_str().unwrap();
        let new_name = format!("{}/{}T.{}", dir_name, name, extension);
        fs::rename(file, &new_name).unwrap();
        let file = PathBuf::from(new_name);
        filetime::set_file_times(&file, original_time, original_time).unwrap();
        renamed_files.push(PathBuf::from(file));
    }
    renamed_files
}