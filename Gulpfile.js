
var fs = require('fs'),
    path = require('path'),
    gulp = require('gulp'),
    babel = require('gulp-babel');

gulp.task('babel', function() {
  return gulp.src(['src/**/*.js'])
      .pipe(babel())
      .pipe(gulp.dest('dst'))
      .on('error', console.error);
});

gulp.task('watch', function() {
  gulp.watch('src/**/*.js', ['babel']);
});

gulp.task('default', ['babel', 'watch']);
gulp.task('build', ['babel']);
