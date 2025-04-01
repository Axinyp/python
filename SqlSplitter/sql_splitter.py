#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import argparse
import re
from pathlib import Path
import sys
import time
import psutil
import datetime

class SQLSplitter:
    def __init__(self, input_file, output_dir, statements_per_file=100):
        """
        初始化SQL分割器
        
        Args:
            input_file (str): 输入的SQL文件路径
            output_dir (str): 输出目录路径
            statements_per_file (int): 每个文件包含的SQL语句数量
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.statements_per_file = statements_per_file
        
        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        
        # 检查文件是否存在
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"输入文件不存在: {input_file}")
            
        # 检查文件大小
        self.file_size = os.path.getsize(input_file)
        if self.file_size > 1024 * 1024 * 1024:  # 1GB
            print(f"警告: 文件大小超过1GB ({self.file_size / (1024*1024*1024):.2f}GB)")
            
        # 初始化进度跟踪
        self.start_time = time.time()
        self.processed_size = 0
        self.last_progress_time = time.time()
        
    def _format_size(self, size):
        """格式化文件大小显示"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.2f}{unit}"
            size /= 1024
        return f"{size:.2f}TB"
        
    def _format_time(self, seconds):
        """格式化时间显示"""
        return str(datetime.timedelta(seconds=int(seconds)))
        
    def _get_memory_usage(self):
        """获取当前进程的内存使用情况"""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024  # 转换为MB
        
    def _update_progress(self, line_size):
        """更新进度显示"""
        self.processed_size += line_size
        current_time = time.time()
        
        # 每0.5秒更新一次进度
        if current_time - self.last_progress_time >= 0.5:
            progress = (self.processed_size / self.file_size) * 100
            elapsed_time = current_time - self.start_time
            speed = self.processed_size / elapsed_time if elapsed_time > 0 else 0
            remaining_size = self.file_size - self.processed_size
            remaining_time = remaining_size / speed if speed > 0 else 0
            memory_usage = self._get_memory_usage()
            
            print(f"\r处理进度: {progress:.1f}% | "
                  f"已处理: {self._format_size(self.processed_size)} | "
                  f"总大小: {self._format_size(self.file_size)} | "
                  f"速度: {self._format_size(speed)}/s | "
                  f"预计剩余时间: {self._format_time(remaining_time)} | "
                  f"内存使用: {memory_usage:.1f}MB", end='')
            
            self.last_progress_time = current_time
        
    def split_sql(self):
        """分割SQL文件"""
        statements = []
        current_statement = []
        current_file_index = 0
        statement_count = 0
        
        # 获取输入文件名
        input_filename = Path(self.input_file).stem
        
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                for line in f:
                    # 更新进度
                    self._update_progress(len(line.encode('utf-8')))
                    
                    # 跳过注释行
                    if line.strip().startswith('--') or line.strip().startswith('/*'):
                        continue
                        
                    current_statement.append(line)
                    
                    # 检查是否包含分号
                    if ';' in line:
                        # 合并当前语句
                        statement = ''.join(current_statement).strip()
                        if statement:
                            statements.append(statement)
                            statement_count += 1
                            
                            # 当达到每个文件的语句数量限制时，写入文件
                            if len(statements) >= self.statements_per_file:
                                self._write_statements_to_file(
                                    statements, 
                                    input_filename, 
                                    current_file_index
                                )
                                statements = []
                                current_file_index += 1
                                
                        current_statement = []
            
            # 处理最后一个语句
            if current_statement:
                statement = ''.join(current_statement).strip()
                if statement:
                    statements.append(statement)
                    statement_count += 1
            
            # 写入剩余的语句
            if statements:
                self._write_statements_to_file(
                    statements, 
                    input_filename, 
                    current_file_index
                )
                current_file_index += 1
                
            # 打印最终统计信息
            total_time = time.time() - self.start_time
            print(f"\n\nSQL文件分割完成！")
            print(f"总处理时间: {self._format_time(total_time)}")
            print(f"平均处理速度: {self._format_size(self.file_size/total_time)}/s")
            print(f"共分割成 {current_file_index} 个文件")
            print(f"总SQL语句数: {statement_count}")
            print(f"输出目录: {self.output_dir}")
            
        except Exception as e:
            print(f"\n处理文件时出错: {str(e)}")
            sys.exit(1)
            
    def _write_statements_to_file(self, statements, input_filename, file_index):
        """将SQL语句写入文件"""
        output_file = os.path.join(
            self.output_dir,
            f"{input_filename}_part{file_index+1:03d}.sql"
        )
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('\n\n'.join(statements))
        except Exception as e:
            print(f"\n写入文件 {output_file} 时出错: {str(e)}")
            sys.exit(1)

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='SQL文件分割工具')
    parser.add_argument('input_file', help='输入的SQL文件路径')
    parser.add_argument('output_dir', help='输出目录路径')
    parser.add_argument(
        '--statements',
        type=int,
        default=100,
        help='每个文件包含的SQL语句数量 (默认: 100)'
    )
    
    # 解析命令行参数
    args = parser.parse_args()
    
    try:
        # 创建SQL分割器实例并执行分割
        splitter = SQLSplitter(args.input_file, args.output_dir, args.statements)
        splitter.split_sql()
    except Exception as e:
        print(f"错误: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 