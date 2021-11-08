'use strict'

const {mapUser, getRandomFirstName, mapArticles} = require('./util')

const students = require('./students.json');

// db connection and settings
const connection = require('./config/connection')
let userCollection, studentCollection, articleCollection
run()

async function run() {
  await connection.connect()
  // await connection.get().dropCollection('users')
  // await connection.get().createCollection('users')
  userCollection = connection.get().collection('users')

  // await connection.get().dropCollection('articles')
  // await connection.get().createCollection('articles')
  articleCollection = connection.get().collection('articles')

  // await connection.get().dropCollection('students')
  // await connection.get().createCollection('students')
  studentCollection = connection.get().collection('students')

  // await example1()
  // await example2()
  // await example3()
  // await example4()
  // await example5()
  // await example6()
  // await example7()
  // await example8()
  // await example9()
  // await example10()
  // await example11()
  // await example12()
  // await example13()
  // await example14()
  // await example15()
  // await example16()
  // await example17()
  await connection.close()
}

// #### Users

// - Create 2 users per department (a, b, c)
async function example1() {
  const departments = ['a', 'a', 'b', 'b', 'c', 'c'];
  try {
    const users = departments.map(d => ({ department: d })).map(mapUser);
    const { result } = await userCollection.insertMany(users);
    console.log(result);
  } catch (err) {
    console.error(err)
  }
}

// - Delete 1 user from department (a)

async function example2() {
  try {
    const { result } = await userCollection.deleteOne({ department: 'a' });
    console.log(result);
  } catch (err) {
    console.error(err)
  }
}

// - Update firstName for users from department (b)

async function example3() {
  try {
    const usersToUpdate = await userCollection.find({ department: 'b' }).toArray();
    const bulkWrite = usersToUpdate.map(user => ({
      updateOne: {
        filter: { _id: user._id },
        update: { $set: { firstName: getRandomFirstName() } }
      }
    }));
    const res = await userCollection.bulkWrite(bulkWrite);
    console.log(res);
  } catch (err) {
    console.error(err)
  }
}

// - Find all users from department (c)
async function example4() {
  try {
    const res = await userCollection.find({ department: 'c' }).toArray();
    console.log(res);
  } catch (err) {
    console.error(err)
  }
}

// #### Articles

// Create 5 articles per each type (a, b, c)
async function example5() {
  const types = ['a', 'a', 'a', 'a', 'a', 'b', 'b', 'b', 'b', 'b', 'c', 'c', 'c', 'c', 'c'];
  try {
    const articles = types.map(t => ({ type: t })).map(mapArticles);
    const { result } = await articleCollection.insertMany(articles);
    console.log(result);
  } catch (err) {
    console.error(err);
  }
}

// Find articles with type a, and update tag list with next value [‘tag1-a’, ‘tag2-a’, ‘tag3’]
async function example6() {
  const tags = ['tag1-a', 'tag2-a', 'tag3'];
  try {
    const { result } = await articleCollection.updateMany({ type: 'a' }, { $set: { tags } });
    console.log(result);
  } catch (err) {
    console.error(err);
  }
}

// Add tags [‘tag2’, ‘tag3’, ‘super’] to other articles except articles from type a
async function example7() {
  const tags = ['tag2', 'tag3', 'super'];
  try {
    const { result } = await articleCollection.updateMany({ type: { $ne: 'a' } }, { $set: { tags } });
    console.log(result);
  } catch (err) {
    console.error(err);
  }
}

// Find all articles that contains tags [tag2, tag1-a]
async function example8() {
  try {
    const res = await articleCollection.find({ 
      tags: { 
        $in: ['tag2', 'tag1-a'],
      } 
    }).toArray();

    console.log(res);
  } catch (err) {
    console.error(err);
  }
}

// Pull [tag2, tag1-a] from all articles
async function example9() {
  try {
    const { result } = await articleCollection.updateMany({}, { 
      $pull: { 
        tags: { 
          $in: ['tag2', 'tag1-a'] 
        } 
      } 
    });

    console.log(result);
  } catch (err) {
    console.error(err);
  }
}

// #### Students

// Import all data from students.json into student collection
async function example10() {
  try {
    const { result } = await studentCollection.insertMany(students);
    console.log(result);
  }
  catch (err) {
    console.error(err);
  }
}

// Find all students who have the worst score for homework, sort by descent
async function example11() {
  try {
    const pipeline = [
      { 
        $unwind: '$scores' 
      },
      { 
        $match: { 
          'scores.type': 'homework' 
        } 
      },
      { 
        $sort: { 
          'scores.score': 1 
        } 
      }
    ];

    const res = await studentCollection.aggregate(pipeline);
    await res.forEach(student => console.log(student));
  } catch (err) {
    console.error(err);
  }
}

// Find all students who have the best score for quiz and the worst for homework, sort by ascending
async function example12() {
  try {
    const pipeline = [
      {
        $sort: {
          'scores.1.score': 1,
          'scores.2.score': -1
        }
      }
    ];

    const res = await studentCollection.aggregate(pipeline);
    await res.forEach(student => console.log(student));
  } catch (err) {
    console.error(err);
  }
}

// Find all students who have best scope for quiz and exam
async function example13() {
  try {
    const pipeline = [
      {
        $addFields: {
          quizScores: {
            $arrayElemAt: ['$scores', 1]
          },
          examScores: {
            $arrayElemAt: ['$scores', 0]
          },
        }
      },
      {
        $addFields: {
          totalScore: {
            $add: ['$examScores.score', '$quizScores.score']
          }
        }
      },
      {
        $sort: {
          totalScore: -1
        }
      }
    ];

    const res = await studentCollection.aggregate(pipeline);
    await res.forEach(student => console.log(student.totalScore));
  } catch (err) {
    console.error(err);
  }
}

// Calculate the average score for homework for all students
async function example14() {
  try {
    const pipeline = [
      {
        $unwind: '$scores',
      },
      {
        $match: {
          'scores.type': 'homework',
        }
      },
      {
        $group: {
          _id: null,
          averageScore: {
            $avg: '$scores.score',
          }
        }
      }
    ];
    
    const res = await studentCollection.aggregate(pipeline);
    await res.forEach(result => console.log(result.averageScore));
  } catch (err) {
    console.error(err);
  }
}

// Delete all students that have homework score <= 60
async function example15() {
  try {
    const pipeline = [
      {
        $unwind: '$scores',
      },
      {
        $match: {
          'scores.type': 'homework',
          'scores.score': {
            $lte: 60
          }
        }
      },
    ];

    const studentsToDelete = await studentCollection.aggregate(pipeline).toArray();

    const arrayOfStudentId = await studentsToDelete.map(student => student._id);

    const { result } = await studentCollection.deleteMany({ _id: { $in: arrayOfStudentId } });

    console.log(result);
    
  } catch (err) {
    console.error(err);
  }
}

// Mark students that have quiz score => 80
async function example16() {
  try {
    const pipeline = [
      { $unwind: '$scores' },
      {
        $match: {
          'scores.type': 'quiz',
          'scores.score': { $gte: 80 }
        }
      }
    ];

    const students = await studentCollection.aggregate(pipeline).toArray();
    
    const studentsId = students.map(student => student._id);

    const { result } = await studentCollection.updateMany({ _id: { $in: studentsId } }, { $set: { marked: true } });

    console.log(result);

  } catch (err) {
    console.error(err);
  }
}

// Write a query that group students by 3 categories (calculate the average grade for three subjects)
//  - a => (between 0 and 40)
//  - b => (between 40 and 60)
//  - c => (between 60 and 100)
async function example17() {
  try {
    const pipeline = [
      {
        $addFields: {
          examScores: {
            $arrayElemAt: ['$scores', 0]
          },
          quizScores: {
            $arrayElemAt: ['$scores', 1]
          },
          homeworkScores: {
            $arrayElemAt: ['$scores', 2]
          },
        }
      },
      {
        $addFields: {
          averageScore: {
            $avg: ['$examScores.score', '$quizScores.score', '$homeworkScores.score'],
          }
        } 
      },
      { 
        $addFields: {
          category: {
            $switch: {
              branches: [
                { case: { $lt: [ '$averageScore', 40 ] }, then: 'a' },
                { case: { $lt: [ '$averageScore', 60 ] }, then: 'b' },
              ],
              default: 'c',
            }
          }
        }
      },
      {
        $group: {
          _id: '$category',
          students: {
            $push: { name: '$name', averageScore: '$averageScore' },
          },
        }
      },
    ];

    const res = await studentCollection.aggregate(pipeline).toArray();

    res.map(group => console.log(group._id, group.students));
  } catch (err) {
    console.error(err);
  }
}